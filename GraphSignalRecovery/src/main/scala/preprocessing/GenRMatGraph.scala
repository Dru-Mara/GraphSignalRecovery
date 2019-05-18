package preprocessing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import scala.runtime.ScalaRunTime._
import org.apache.spark.storage.StorageLevel
import java.io._
import scala.math._
import org.apache.spark.graphx.lib
import org.apache.spark.mllib.random.RandomRDDs._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import Array._
import scala.collection.mutable.HashMap
import javax.swing._
import scala.swing._

/*
 * Generate a synthetic graph using the R-Mat model. (http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf)
 * Vertices are initialized randomly with signals in the range 1-N_clust+1.
 * The weights are set as (1/NodeDiff+1)+1 (so they are in range: 1.0-2.0)
 * The graph has a powerlaw distribution of degrees. 
 * 
 */

object GenRMatGraph {

    // Main function
  def main(args: Array[String]) {
    
    if (args.length != 5) {
      System.err.println("Usage: GenRMatGraph <output-path> <numVert> <numEdges> <numClust> <numPart>")
      System.err.println("Eg: GenRMatGraph ./Datasets/RMatgraph_200K_1M_8/ 200000 1000000 8 16")
      System.exit(1)
    }
    
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val outputPath = args(0)
    val N_vert = args(1).toInt
    val N_edges = args(2).toInt
    val N_clust = args(3).toInt-1
    val numPart = args(4).toInt
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("GenRMatGraph").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // Start time count 
    val startp = System.nanoTime
    
    // Create an R-Mat graph
    val graph0 = GraphGenerators.rmatGraph(sc, N_vert, N_edges).cache
    
    // Compute the connected components
    val graphcc = graph0.connectedComponents().cache()  // (VertexID, LowestVertexIDinCC)
    
    // Find VertexID of main cc
    val maincc = graphcc.vertices.values.map(x => (x,1)).reduceByKey(_ + _).map(item => item.swap).max // (numElems, minVertexID)
    println("Main connected component (numVertices, minVertexID): ", maincc)
    
    // Join graph0 and graphcc and keep only elements from main cc
    val graph1 = graph0.outerJoinVertices(graphcc.vertices){ (vid, vd, cc) => (vd, cc.getOrElse(0.0)) }
    .subgraph(vpred = { case (vid, cc) => cc._2 == maincc._2}).mapVertices((vid,vd) => vd._1)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic info
    println(" Graph CC: ")
    helperFuncs.graphInfo.showGraph(graph1, 10)
    
    /*
     * OLD implementation!
     * LP finds always one huge community (90%) and a lot of tiny ones, not good for signal initialization
     * 
    // Run LP to detect communities and label nodes in them
    var lp_labels = graph1.vertices.mapValues(x=> List[Long]()).cache
    for (i <- 1 to LP_runs){
        val lp_i = lib.LabelPropagation.run(graph1, LP_MAX_ITER).vertices.cache
        println("Communities found in iter " + i.toString() + ": " + lp_i.values.distinct().count)
        lp_labels = lp_labels.innerZipJoin(lp_i)((vid,lst,minVid)=> lst ++ List(minVid)).cache
    }
    
    // Take signal of graph as communityID of vertices if in same community more than half the LP-cd's
    val signals = lp_labels.mapValues{x => 
      val a = x.groupBy(identity).maxBy(_._2.size)
      if (a._2.size > LP_runs/2.0) a._1 else 0.0    // if vertex has same community in more than half the runs then give it that community
    }.map(x => x.swap).join(signals.values.distinct().zipWithIndex()).values.cache
    
    // Buid a new graph with the desired signal values and set W of edges to 2 if str.attr == dest.attr else 1
    val graph2 = Graph(signals.mapValues(x => x.toDouble), graph1.edges).mapTriplets{ triplet =>
      if (triplet.srcAttr == triplet.dstAttr) 2.0 else 1.0
    }
    * 
    */
    
    // Generate N_clust signal values in range [1...N_clust+1]
    // Generate weights as (1/NodeDiff+1)+1 (weights are scaled up to range 1.0-2.0)
    val graph2 = Graph(uniformRDD(sc, graph1.vertices.count, numPart).map(v => (N_clust * v).toInt).zipWithIndex().map(x => (x._2,x._1.toDouble + 1.0)), graph1.edges)
    .mapTriplets{ triplet =>
      (1.0 / (abs(triplet.srcAttr - triplet.dstAttr) + 1.0)) + 1.0
    }.cache         
    
    // Show some basic info
    println(" Graph 2: ")
    helperFuncs.graphInfo.showGraph(graph2, 10)
    
    // Show total preprocessing time
    val endp = System.nanoTime
    println("Preprocessing time (sec.): ", (endp-startp)/1000000000.0)
    
    // Save the graph to files
    helperFuncs.graphInfo.saveGraph(graph2, outputPath)
	  
    // Stop Spark
    sc.stop()
    
  }

  
}
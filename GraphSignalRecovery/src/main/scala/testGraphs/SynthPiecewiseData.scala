package testGraphs

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.runtime.ScalaRunTime._
import org.apache.spark.storage.StorageLevel
import java.io._
import scala.math._
import org.apache.spark.graphx.lib

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import Array._
import scala.collection.mutable.HashMap


/*
 * Testing file for the syntetic piecwise dataset. The graph is generated as a chain graph
 * of the required number N of vertices. It will contain N-1 edges. The signal values of the vertices
 * alternate between two values 1.0 and 5.0 every "period" nodes. Gaussian error can be added to the 
 * signal values. 
 * 
 */


object SynthPiecewiseData {

  
  // Main function
  def main(args: Array[String]) {
    
    if (args.length != 4) {
      System.err.println("Usage: SynthPiecewiseData <output-path> <MAX_ITER> <ALG> <numPart>")
      System.err.println("Eg: SynthPiecewiseData ./SLPoutput/ 100 SLP_v2 16")
      System.exit(1)
    }
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val outputPath = args(0)    // "/home/dru/Escritorio/LP_synth_100"
    val degDistPath = outputPath+"synthPiecewise-degrees.csv"  
    val signalDistPath = outputPath+"synthPiecewise-signals.csv" 
    val MAX_ITER = args(1).toInt
    val alg = args(2)
    val numPart = args(3).toInt
    val N = 1000000    // number of vertices
    val period = 10     // period, as number of nodes, of the square waves
    val fraction = 0.2 //fraction of samples
    val unknownLabel = 0.0    //flags elements with unknown signal value
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Sparse Label Propagation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir(outputPath+"Checkpoint")
    
    // Start time count 
    val startp = System.nanoTime
    
    // Create an RDD for the vertices
    // RDD[(vertexID, signal_val)]
    // Signals vary from 1.0 to 5.0 with the specified period
    val vertices: RDD[(VertexId, Double)] = sc.parallelize(0 to N-1)
    .map(x => if ((x/period).toInt % 2 == 0) (x.toLong, 1.0) else (x.toLong, 5.0) )
    .repartition(numPart).cache //+ math.random/2.0
    
    // Create an RDD for the Edges
    // RDD[Edge[weight]]   :weight of all the edges is set to 2 except in the changes
    val edges: RDD[Edge[Double]] = vertices.filter(x => x._1 != N-1)
    .map(x => Edge(x._1, x._1 + 1, if (x._1 != 0 && ((x._1+1.0) % period) == 0 ) 1.0 else 2.0 )).repartition(numPart)
    
    // Create the chain Graph
    val graph = Graph(vertices,edges)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic info
    println(" Graph: ")
    helperFuncs.graphInfo.showGraph(graph, 10)
    
    // Show total preprocessing time
    val endp = System.nanoTime
    println("Preprocessing time (sec.): ", (endp-startp)/1000000000.0)
    
    /*
    // Compute Efficient Sparsity of graph (smaller than num edges?)
    val edge_diff = graph.triplets.map(triplet => abs(triplet.dstAttr-triplet.srcAttr) ).cache
    val eff_sp = edge_diff.reduce(_+_) / edge_diff.max
    println("Efficient Sparsity of graph: " + eff_sp.toString())
    
    // Compute LP perf on graph
    val vert_sum = graph.vertices.values.map(x => pow(x,2.0)).reduce(_+_)
    val pow_edge_diff = edge_diff.map(x => pow(x,2.0)).reduce(_+_) / vert_sum
    println("LP performance on graph: " + pow_edge_diff.toString()) 
    
    // Compute the degrees
    println("Sample of node degrees: ")
    graph.degrees.take(5).foreach(println)
    
    // Compute degree distribution
    helperFuncs.graphInfo.degDist(graph, degDistPath)
    
    // Compute the frequency of the avg_ratings
    helperFuncs.graphInfo.vertSignalDist(graph, signalDistPath)
    */
    
    // Create RDD of real labels to compute MSE
    val real_l: VertexRDD[Double] = VertexRDD(graph.vertices.filter(x => x._2 != unknownLabel)
        .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache
    //real_l.sortByKey(true, 1).map(e => e._1.toString() + "," + e._2.toString()).coalesce(1).saveAsTextFile(outputPath+"real_l")
    
    // Create set S of samples
    //val S: VertexRDD[Double] = VertexRDD(real_l.filter(x => x._1 == 0.0.toLong || x._1 == (N-1).toLong)
    //    .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache   //takes first and last elems as samples
    //val S: VertexRDD[Double] = VertexRDD(real_l.filter(x => (x._1-1) % period == 0.0.toLong)
    //    .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache   //takes as sample 1 elem from each piecewise const part
    val S: VertexRDD[Double] = VertexRDD(real_l.sample(false, fraction)
        .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache     //takes random sample  
    println("Num Samples in S: " + S.count.toString())
    S.sortByKey(true, 1).map(e => e._1.toString() + "," + e._2.toString()).coalesce(1).saveAsTextFile(outputPath+"samples")
    
    // Use selected algorithm to predict node labels
    alg match {
      case "LP_cd" =>
        val start = System.nanoTime
        val labels = algorithms.LP_cd.run(graph, S, MAX_ITER, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case "LP_sr" =>
        val start = System.nanoTime
        val labels = algorithms.LP_sr.run(graph, S, real_l, MAX_ITER, outputPath, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case "Nw_lasso" =>
        val start = System.nanoTime
        val labels = algorithms.Nw_lasso.run(graph, S, real_l, MAX_ITER, outputPath, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case "SLP_v2" =>
        val start = System.nanoTime
        val labels = algorithms.SLP_v2.run(sc, graph, S, real_l, MAX_ITER, outputPath, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case _ => "ERROR: Unknown algorithm!" 
    }
    
    // Save the graph to files
    //helperFuncs.graphInfo.saveGraph(graph, outputPath)

    // Stop Spark
    sc.stop()
    
  }
}
package testGraphs

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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import Array._
import scala.collection.mutable.HashMap
import javax.swing._
import scala.swing._


/*
 * Testing file for f/b segmentation on a real image. The image is converted to a grid graph using 4-connected 
 * neighbourhood (total of N nodes). Each node has a signal value of -1 if bg or 1 if fg.
 * This preprocessing is done in matlab! Here we just import the edgelist and vertex values.
 * 
 * The weights represent the difference between connected RGB pixels (large difference = small weight)
 * The sampling consists of a region of known bg (R1) and a region of known fg (R2), the task is to detecr R3 
 * 
 */

object ImageData {
  
  // Main function
  def main(args: Array[String]) {
    
    if (args.length != 7) {
      System.err.println("Usage: ImageData <vertices-path> <edges-path> <sample-path> <output-path> <MAX_ITER> <ALG> <numPart>")
      System.err.println("Eg: ImageData ./vertices.txt ./edgelist.txt ./samples.txt ./SLPoutput/ 100 SLP_v2 8")
      System.exit(1)
    }
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val vert_path = args(0)
    val edge_path = args(1)
    val samp_path = args(2)
    val outputPath = args(3)    
    val degDistPath = outputPath+"synthPiecewise-degrees.csv"  
    val signalDistPath = outputPath+"synthPiecewise-signals.csv" 
    val MAX_ITER = args(4).toInt
    val alg = args(5)
    val numPart = args(6).toInt
    val unknownLabel = 0.0    //flags elements with unknown signal value
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Sparse Label Propagation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir(outputPath+"Checkpoint")
    
    // Start time count 
    val startp = System.nanoTime
    
    // Read vertices RDD
    // RDD[(vertexID, signal_val)]
    // Signals are -1.0 if pixel if from bg, 1.0 if is from fg, else 0.0 if unknown
    // All points in vertices are labeled since is the true segmented image what we use
    val vertices: RDD[(VertexId, Double)] = sc.textFile(vert_path, numPart).map{line => 
      val array = line.split(",")
      (array(0).toLong,array(1).toDouble)
    }
    
    // Read edges RDD
    // RDD[Edge[weight]]
    val edges: RDD[Edge[Double]] = sc.textFile(edge_path, numPart).map{ line => 
      val array = line.split(",")
      Edge(array(0).toLong,array(1).toLong,array(2).toDouble)
    }
    
    // Create the Grid Graph
    val graph = Graph(vertices,edges).partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
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
    
    // Read sample set S
    // RDD[(vertexID, signal_val)]
    // Signals are -1.0 if sample if from bg, 1.0 if is from fg
    val S: VertexRDD[(Double)] = VertexRDD(sc.textFile(samp_path, numPart).map{line => 
      val array = line.split(",")
      (array(0).toLong,array(1).toDouble)
    }.repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache
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

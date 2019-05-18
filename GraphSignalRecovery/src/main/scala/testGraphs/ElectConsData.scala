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
 * Testing file for the electricity consumption dataset. The graph is a chain graph where nodes
 * represent instant power consumption in a house along the time. The graph contains N vertices
 * and N-1 edges. The graph is not weighted, but it can be made so by removing some values from the
 * chain and setting the edge weight to 1-(timestampdiff) between points.
 * 
 */

object ElectConsData {
  
  
  // Main function
  def main(args: Array[String]) {
    
    if (args.length != 5) {
      System.err.println("Usage: ElectConsData <input-path> <output-path> <MAX_ITER> <ALG> <numPart>")
      System.err.println("Eg: ElectConsData ./household_power_consumption.txt ./SLPoutput/ 100 SLP_v2 16")
      System.exit(1)
    }
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val inputPath = args(0)     
    val outputPath = args(1)    
    val degDistPath = outputPath+"ElectGraph-degrees.csv"
    val signalDistPath = outputPath+"ElectGraph-signals.csv" 
    val MAX_ITER = args(2).toInt
    val alg = args(3)
    val numPart = args(4).toInt
    val fraction = 0.2         // fraction of samples
    val unknownLabel = -1.0    // flags elements with unknown signal value
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Sparse Label Propagation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir(outputPath+"Checkpoint")
    
    // Start time count 
    val startp = System.nanoTime
    
    // Read dataset remove first row and create an RDD for the vertices
    // RDD[(vertexID, Elect_cons)]
    val vertices: RDD[(VertexId, Double)] = sc.textFile(inputPath, numPart).filter(line => !line.contains("Date")).map( line => line.split(";"))
    .map{ array => if (array(2).trim() == "?") unknownLabel else array(2).trim().toDouble }
    .zipWithIndex().map(x => x.swap).repartition(numPart).cache    
    
    // Count number of elements in the dataset
    val N = vertices.count
    
    // Create an RDD for the Edges
    // RDD[Edge[weight]]   :weight of all the edges is set to 1
    val edges: RDD[Edge[Double]] = vertices.filter(x => x._1 != N-1).map(x => Edge(x._1, x._1 + 1, 1.0)).repartition(numPart)
    
    // Create the chain Graph
    val graph = Graph(vertices,edges)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    val aux = graph.vertices.filter(x => x._2 != unknownLabel).sample(false, 0.8).zipWithIndex().map(x => x.swap).cache
    val N2 = aux.count
    println("N2:", N2.toString())
    val vertices2: RDD[(VertexId, Double)] = aux.map(x => (x._1, x._2._2))
    val edges2 = aux.filter(x => x._1 != N2).map(x => ((x._1+1.0).toLong, x._2._1)).join(aux) //we change the vertID of x to x+1 and join with vert x+1 to get its value
    .map(x => Edge(x._1-1, x._1, 1.0 + 1.0/(x._2._2._1 - x._2._1)) )  //diff = (OLDvertIDx+1 - OLDvertIDx) and set edge w to 1+1/diff
    val graph1 = Graph(vertices2,edges2)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic info
    println(" Graph1: ")
    helperFuncs.graphInfo.showGraph(graph1, 10)
    
    // Show some basic info
    println(" Graph: ")
    helperFuncs.graphInfo.showGraph(graph, 10)
    
    // Show total preprocessing time
    val endp = System.nanoTime
    println("Preprocessing time (sec.): ", (endp-startp)/1000000000.0)
    
    // Some points are missing so this measure will not be precise
    // Compute Efficient Sparsity of graph (smaller than num edges?)
    val edge_diff = graph.triplets.map(triplet => abs(triplet.dstAttr-triplet.srcAttr) ).cache
    val eff_sp = edge_diff.reduce(_+_) / edge_diff.max
    println("Efficient Sparsity of graph: " + eff_sp.toString())
    
    // Compute LP perf on graph
    val vert_sum = graph.vertices.values.map(x => pow(x,2.0)).reduce(_+_)
    val pow_edge_diff = edge_diff.map(x => pow(x,2.0)).reduce(_+_) / vert_sum
    println("LP performance on graph: " + pow_edge_diff.toString()) 
    
    /*
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
    val S: VertexRDD[Double] = VertexRDD(real_l.sample(false, fraction)
        .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache    
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
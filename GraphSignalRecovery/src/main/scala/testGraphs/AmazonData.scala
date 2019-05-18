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
 * Testing file for the Amazon dataset. A graph is build from the preprocessed data
 * The graph has a power law distribution of the degrees and its unweighted and undirected.
 * Several algorithms can be tested depending on input parameter
 */

object AmazonData {
  
  
  // Main function
  def main(args: Array[String]) {
    
    if (args.length != 5) {
      System.err.println("Usage: AmazonData <input-path> <output-path> <MAX_ITER> <ALG> <numPart>")
      System.err.println("Eg: AmazonData ./Amazon-meta-clean ./SLPoutput/ 100 SLP_v2 8")
      System.exit(1)
    }
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val inputPath = args(0)    
    val outputPath = args(1)                  
    val degDistPath = outputPath + "Amazon-meta-degrees.csv"
    val avgRatingDistPath = outputPath + "Amazon-meta-ratings.csv"
    val MAX_ITER = args(2).toInt
    val alg = args(3)
    val numPart = args(4).toInt
    val fraction = 0.2        //fraction of samples
    val unknownLabel = 0.0    //flags elements with unknown signal value
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Sparse Label Propagation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir(outputPath+"Checkpoint")
    
    // Start time count 
    val startp = System.nanoTime
    
    // Read dataset and split the 3 fields
    val file = sc.textFile(inputPath).map( line => line.split(","))
    .map{ array => (array(0).trim(), array(1).toDouble, array(2).split("""\s+""").map(_.trim()) )}.cache

    // Create indexing for the vertices (string, Long)
    val indexes = file.flatMap{ triplet => triplet._3.drop(2).map(x => x.trim()) }.union(file.map(x => x._1.trim())).distinct().zipWithIndex().cache
    
    // Create an RDD for the vertices
    // RDD[(ASIN, avg_rating)]
    val vertices: RDD[(VertexId, Double)] = indexes.leftOuterJoin(file.map(x => (x._1,x._2))).map(x => (x._2._1,x._2._2.getOrElse(0.0)) )
    .repartition(numPart)
    
    // Create an RDD of triplets from file (source_string,dest_string,w)
    val triplets = file.filter(triplet => triplet._3(1) != "0").flatMap{ triplet => triplet._3.drop(2).map(x => (x,(triplet._1,1.0))) }
    
    // Create an RDD for the Edges
    // RDD[Edge[weight]]   :weight of all the edges is set to 1
    // Remove elements with no related products (they will not generate edges)
    // Drop(2) removes the first empty element of [related_products] and the second which is length([related_products])
    val edges: RDD[Edge[Double]] = triplets.leftOuterJoin(indexes).map(x => (x._2._1._1, (x._2._2.getOrElse(-1.toLong),x._2._1._2))).leftOuterJoin(indexes)
        .map(x => Edge(x._2._1._1,x._2._2.getOrElse(-1.toLong),x._2._1._2)).repartition(numPart)
    
    // Create the Graph
    // Keep only the vertices with known rating and "&& vid % 4 == 0" for subgraph of 6150 nodes
    val graph = Graph(vertices,edges) //.subgraph(vpred = {case (vid, cc) => cc != 0})
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic data
    println(" Graph: ")
    helperFuncs.graphInfo.showGraph(graph, 10)
    
    // Compute the connected components
    val graphcc = graph.connectedComponents().cache()  // (VertexID, LowestVertexIDinCC)
    
    // Find VertexID of main cc
    val maincc = graphcc.vertices.values.map(x => (x,1)).reduceByKey(_ + _).map(item => item.swap).max // (numElems, minVertexID)
    println("Main connected component (numVertices, minVertexID): ", maincc)
    
    // Join graph and graphcc and keep only elements from main cc
    val graph1 = graph.outerJoinVertices(graphcc.vertices){ (vid, vd, cc) => (vd, cc.getOrElse(0.0)) }
    .subgraph(vpred = { case (vid, cc) => cc._2 == maincc._2}).mapVertices((vid,vd) => vd._1)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic data
    println(" graph1: Restricted to maincc Graph: ")
    helperFuncs.graphInfo.showGraph(graph1, 10)
    
    // Show total preprocessing time
    val endp = System.nanoTime
    println("Preprocessing time (sec.): ", (endp-startp)/1000000000.0)
    
    /*
     * Some points are missing so this measure will not be precise
    // Compute Efficient Sparsity of graph1 (smaller than num edges?)
    val edge_diff = graph1.triplets.map(triplet => abs(triplet.dstAttr-triplet.srcAttr) ).cache
    val eff_sp = edge_diff.reduce(_+_) / edge_diff.max
    println("Efficient Sparsity of graph1: " + eff_sp.toString())
    
    // Compute LP perf on graph1
    val vert_sum = graph1.vertices.values.map(x => pow(x,2.0)).reduce(_+_)
    val pow_edge_diff = edge_diff.map(x => pow(x,2.0)).reduce(_+_) / vert_sum
    println("LP performance on graph1: " + pow_edge_diff.toString()) 
    
    // Compute the degrees
    println("Sample of node degrees: ")
    graph1.degrees.take(5).foreach(println)
    
    // Compute degree distribution
    helperFuncs.graphInfo.degDist(graph1, degDistPath)
    
    // Compute the frequency of the avg_ratings
    helperFuncs.graphInfo.vertSignalDist(graph1,avgRatingDistPath)
    */
    
    // Create RDD of real labels to compute MSE
    val real_l: VertexRDD[Double] = VertexRDD(graph1.vertices.filter(x => x._2 != unknownLabel)
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
        val labels = algorithms.LP_cd.run(graph1, S, MAX_ITER, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case "LP_sr" =>
        val start = System.nanoTime
        val labels = algorithms.LP_sr.run(graph1, S, real_l, MAX_ITER, outputPath, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case "SLP_v2" =>
        val start = System.nanoTime
        val labels = algorithms.SLP_v2.run(sc, graph1, S, real_l, MAX_ITER, outputPath, numPart)
        val end = System.nanoTime
        println(alg + " execution time (s): ", (end-start)/1000000000.0)
        labels.sortByKey().map(e => e._1.toString() + "," + e._2.toString() + "," + (Math.round(e._2/0.5) * 0.5).toString())
        .coalesce(1).saveAsTextFile(outputPath+alg+"_labels")
      case _ => "ERROR: Unknown algorithm!" 
    }
    
    // Save the graph to files
    //helperFuncs.graphInfo.saveGraph(graph1, outputPath)

    // Stop Spark
    sc.stop()
    
  }
}
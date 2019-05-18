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
 * Testing file for the 3D road network dataset. The graph is a power law graph where nodes
 * represent road intersections and edges roads between them. The graph is weighted and weights
 * represent distance of the segments of road (km). Vertices have signals representing the altitude
 * at that point (meters). 
 * Altitude is measured in meters and is not very precise. Points with same lat,long have different 
 * altitude values in the decimal part, so altitude will be taken as int
 * Altitude can be negative (below sea level)
 * Because of the structure of the graph I can not run connectedComponents on it...
 * 
 */

object Road3DData {
  
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    var p = 0.017453292519943295    // Math.PI / 180
    var a = 0.5 - Math.cos((lat2 - lat1) * p) * 0.5 +
          Math.cos(lat1 * p) * Math.cos(lat2 * p) * 
          (1 - Math.cos((lon2 - lon1) * p)) * 0.5
    12742 * Math.asin(Math.sqrt(a)) // 2 * R; R = 6371 km
  }
  
  // Main function
  def main(args: Array[String]) {
    
    if (args.length != 5) {
      System.err.println("Usage: Road3DData <input-path> <output-path> <MAX_ITER> <ALG> <numPart>")
      System.err.println("Eg: Road3DData ./3dNetwork.txt ./SLPoutput/ 100 SLP_v2 8")
      System.exit(1)
    }
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Variables
    val inputPath = args(0)  
    val outputPath = args(1)  
    val degDistPath = outputPath+"Road3d-degrees.csv"  
    val signalDistPath = outputPath+"Road3d-ratings.csv" 
    val MAX_ITER = args(2).toInt
    val alg = args(3)
    val numPart = args(4).toInt
    val fraction = 0.2            //fraction of samples
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Road3DData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir(outputPath+"Checkpoint")
    
    // Start time count 
    val startp = System.nanoTime
    
    // Read road file and create RDD of ((roadID, long, lat, alt), VertexID)
    val file = sc.textFile(inputPath, numPart).map{line => 
      val array = line.split(",")
      (array(0).toLong, array(1).toDouble, array(2).toDouble, Math.round(array(3).toDouble).toInt)
    }.zipWithIndex().cache
    
    // (long+lat,[(vertID1,alt1), (vertID2,alt2) ... ]) all this vertexID are the same point and altitudes should be aprox equal
    // ([(vertID1,alt1), (vertID2,alt2) ... ], newVertexID)
    val vl_file = file.map{tupl => (tupl._1._2.toString + "," + tupl._1._3.toString, Array((tupl._2, tupl._1._4))) }
    .reduceByKey((a, b) => a ++ b).values.zipWithIndex().cache
    
    //vl_file.map(x => (x._2, x._1)).sortByKey(true, 1).map(e => e._1.toString() + "," + e._2.mkString(","))
    //.coalesce(1).saveAsTextFile(outputPath+"intersections")
    
    // Create an RDD for the vertices
    // RDD[(vertexID, alt)] 
    val vertices: RDD[(VertexId, Double)] = vl_file.map{ tupl => (tupl._2,tupl._1(0)._2.toDouble)}
    
    // This has to be joined with the edgelist to make equal points from two roads have the same id
    val aux = vl_file.flatMap{ tupl => 
      tupl._1.map(x => (x._1,tupl._2)) // (vertexID, newVertexID)
    }.cache
    
    // Compute the preliminary edgelist file with independent roads
    // (SrcVertID, (DstVertID, dist))
    val el_file = file.map(tupl => (tupl._1._1, Array((tupl._2, tupl._1._2, tupl._1._3)))).reduceByKey((a,b) => a ++ b).values
    .flatMap{x => 
      val a = x.sortBy(x => x._1)
      val b = a.drop(1)
      a.dropRight(1).zip(b)
    }.map{x => 
      (x._1._1, (x._2._1, distance(x._1._2, x._1._3, x._2._2, x._2._3) ))
    }.cache
    
    val maxDist = el_file.map(x => x._2._2).max() + 0.00001
    
    // Create an RDD for the Edges
    // RDD[Edge[weight]]
    val edges: RDD[Edge[Double]] = el_file.join(aux).values.map(x => (x._1._1,(x._1._2,x._2))).join(aux).values
    .map(x => Edge(x._1._2, x._2, maxDist - x._1._1)).repartition(numPart)
    
    // Create the Graph
    val graph = Graph(vertices,edges)
    .partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
    
    // Show some basic data
    println(" Graph: ")
    helperFuncs.graphInfo.showGraph(graph, 10)
    
    // Show total preprocessing time
    val endp = System.nanoTime
    println("Preprocessing time (sec.): ", (endp-startp)/1000000000.0)
    
    /*
    // Compute Efficient Sparsity of graph1 (smaller than num edges?)
    val edge_diff = graph.triplets.map(triplet => abs(triplet.dstAttr-triplet.srcAttr) ).cache
    val eff_sp = edge_diff.reduce(_+_) / edge_diff.max
    println("Efficient Sparsity of graph1: " + eff_sp.toString())
    
    // Compute LP perf on graph1
    val vert_sum = graph.vertices.values.map(x => pow(x,2.0)).reduce(_+_)
    val pow_edge_diff = edge_diff.map(x => pow(x,2.0)).reduce(_+_) / vert_sum
    println("LP performance on graph1: " + pow_edge_diff.toString()) 
    
    // Compute the degrees
    println("Sample of node degrees: ")
    graph.degrees.take(5).foreach(println)
    
    // Compute degree distribution
    helperFuncs.graphInfo.degDist(graph, degDistPath)
    
    // Compute the frequency of the avg_ratings
    helperFuncs.graphInfo.vertSignalDist(graph, signalDistPath)
    */
    
    // Create RDD of real labels to compute MSE
    val real_l: VertexRDD[Double] = VertexRDD(graph.vertices
        .repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache
    real_l.sortByKey(true, 1).map(e => e._1.toString() + "," + e._2.toString()).coalesce(1).saveAsTextFile(outputPath+"real_l")
    
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
    //helperFuncs.graphInfo.saveGraph(graph1, outputPath)

    // Stop Spark
    sc.stop()

  }
}
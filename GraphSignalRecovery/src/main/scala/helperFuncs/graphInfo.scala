package helperFuncs

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
 * Helper functions that present different information of the provided graph.
 * From storing the graph to degree distribution or signal distribution methods are provided.
 * 
 */

object graphInfo {
  
  // Saves the adj matrix, edge list and vertex ratings of given graph
  def saveGraph(g: Graph[_,_], path: String) = {
    val xpath = path+"vertex_signal"
    val apath = path+"adj_list"
    val epath = path+"edge_list"
    g.vertices.sortByKey(true, 1).map(e => e._1.toString() + "," + e._2.toString()).coalesce(1).saveAsTextFile(xpath)
    g.collectNeighbors(EdgeDirection.Either).mapValues(x => x.map(tupl => tupl._1).mkString(",")).sortByKey(true, 1).coalesce(1).saveAsTextFile(apath)
    g.edges.map { x => x.srcId.toString()+","+x.dstId.toString()+","+x.attr.toString()}.coalesce(1).saveAsTextFile(epath)
  }
  
  // Presents some information about the given graph
  // TODO include MAX degree and AVG degree
  def showGraph(g: Graph[_,_], Nsamp: Int) = {
    println("Number of vertices: ", g.numVertices)
    println("Number of edges: ", g.numEdges)
    println("Sample of graph vertices: ")
    g.vertices.take(Nsamp).foreach(println)
    println("Sample of graph edges: ")
    g.edges.take(Nsamp).foreach(println)
  }
  
  // Computes the degree distribution of the given graph
  def degDist(g: Graph[_,_], path: String) = {
    val deg_dist = g.degrees.map(item => item.swap).countByKey()
    println("Degree distribution (degree, frequency): ")
    deg_dist.foreach(println)
    
    // Write degree distribution to a file
    val pw = new PrintWriter(new File(path))
    pw.write("Degree, Frequency\n")
    deg_dist.foreach(x => pw.write(x._1.toString() + "," + x._2.toString() + "\n"))
    pw.close
  }
  
  // Computes the frequency of the vertex signal values of the given graph
  def vertSignalDist(g: Graph[_,_], path: String) = {
    val signal_dist = g.vertices.map(tupl => (tupl._2,1)).reduceByKey(_ + _).collect() // (avgRating, frequency)
    println("Signals distribution (signal, frequency): ")
    signal_dist.foreach(println)
    
    // Write frequency of signals
    val pw2 = new PrintWriter(new File(path))
    pw2.write("Signal, Frequency\n")
    signal_dist.foreach(x => pw2.write(x._1.toString() + "," + x._2.toString() + "\n"))
    pw2.close
  }
  
}
package algorithms

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
 * Distributed implementation of the SLP algorithm presented in 
 * "Semi-supervised Learning via Sparse Label Propagation" by Jung et. al.  
 * Uses normalization by sum of elements in columns or rows of D
 * Contains a lambda parameter to weight between the updates of primal and dual variables 
 * The algorithm is suitable for analyzing weighted graphs.
 * 
 */

object SLP_v2 {
  
  // Runs SLP algorithm on the given dataset and returns the new labels
  def run(sc: SparkContext, g: Graph[Double,Double], S: VertexRDD[Double], real_l: VertexRDD[Double], MAX_ITER: Int, path: String, numPart:Int) = {
    
    // Initialize variables
    val lambda = 1.0 // around 1.5
    
    // Precompute norm of real signal values
    val norm_x = sqrt(real_l.values.map(x => pow(x,2.0)).reduce(_+_))
    val mses = Array.ofDim[Double](MAX_ITER)
    
    // Keep only elements of set S in input graph
    var X = g.outerJoinVertices(S)( (vid,v1,v2) => v2 match {
          case Some(y) => y       // if values in sampling set x(value) = rating
          case None => 0.0        // else x(values) = 0 "unknown rating"
    }).mapTriplets(triplet => 0.0).cache //initialize all edge signals to 0.0
    X.triplets.take(10)
    
    // Some more variables
    var z_k: VertexRDD[Double] = X.vertices.mapValues(vd => 0.0).cache
    var x_k: VertexRDD[Double] = X.vertices.cache
    var x_k1: VertexRDD[Double] = null
    var xhat_k: VertexRDD[Double] = X.vertices.mapValues(vd => 0.0).cache
    var r: VertexRDD[Double] = null
    z_k.count()
    x_k.count()
    xhat_k.count()
    
    // Initialize weight matrix W (as per node signed vector)
    val W = g.collectEdges(EdgeDirection.Either).mapValues( (vid,vd) => ((1.0/(lambda * vd.map(x => abs(x.attr)).reduce(_+_))), vd.sortBy { x => (x.srcId, x.dstId) }.map(x => if(x.srcId == vid) -x.attr else x.attr)) ).cache
    
    // Loop for MAX_ITER
    var startl = 0.0
    var endl = 0.0
    var k = 0
    while (k < MAX_ITER) {
      
      // Take iteration time
      startl = System.nanoTime
      
      // Compute y_k+1
      X = Graph(z_k.repartitionAndSortWithinPartitions(new HashPartitioner(numPart)).localCheckpoint(),
          g.joinVertices(z_k)( (vid,v1,v2) => v2).mapTriplets{triplet => 
             ((lambda/(2.0 * abs(triplet.attr))) * (triplet.dstAttr-triplet.srcAttr)) // works better with 2.0 * abs(triplet.attr) but theoretically not correct
          }.edges.innerJoin(X.edges){ (vid1,vid2,vd1,vd2) => 
             val edgeSignal = vd2 + vd1
             (1.0/max(abs(edgeSignal),1.0)) * edgeSignal
          }.localCheckpoint()
      ).partitionBy(PartitionStrategy.CanonicalRandomVertexCut, numPart).cache
      X.triplets.take(10)
      z_k.unpersist(true)
      
      // Compute r
      r = X.collectEdges(EdgeDirection.Either).leftZipJoin(W){(vid,v1,v2) => v2.get._1 * v1.sortBy{ x => (x.srcId, x.dstId) }.zip(v2.get._2).map(x => x._1.attr * x._2).reduce(_+_)}
      .innerZipJoin(x_k)( (vid,v1,v2) => v2 - v1)
      
      // Compute x_k+1
      x_k1 = VertexRDD(r.leftJoin(S)( (vid,v1,v2) => v2 match {
          case Some(y) => y
          case None => v1
        }
      ).repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).cache
      x_k1.count()
        
      // Compute z_k+1
      z_k = x_k1.innerJoin(x_k)( (vid,v1,v2) => 2.0 * v1 - v2).cache
      z_k.count()
      x_k.unpersist(true)
      
      // Compute xhat_k+1
      xhat_k = xhat_k.innerJoin(x_k1)( (vid,v1,v2) => v1 + v2).localCheckpoint
      xhat_k.count()
      
      // Compute mse
      mses(k) = sqrt(xhat_k.innerJoin(real_l)((vid,v1,v2) => pow(v2 - (1.0/(k+1))*v1 , 2.0)).values.reduce(_ + _))
      mses(k) = pow(mses(k)/norm_x,2.0)
      println("MSE = " + mses(k).toString())
      
      // Update k
      k = k + 1
      
      // Remove unnecessary RDDs
      x_k = x_k1.cache
      x_k.take(5)
      x_k1.unpersist(true)
      
      // Print iteration time/info
      endl = System.nanoTime
      println("Iteration: " + k.toString() + ", Iter time: " + ((endl-startl)/1000000000.0).toString())
    }
    xhat_k = xhat_k.mapValues(x => 1.0/MAX_ITER * x)
    // Write MSEs  
    val pw = new PrintWriter(new File(path+"MSEs.csv"))
    pw.write("MSEs \n")
    mses.foreach(x => pw.write(x.toString() + "\n"))
    pw.close
    xhat_k
  }
  
}
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
 * Implementation of the Label Propagation algorithm for signal recovery or semi-supervised LP. 
 * The existing labels are extended to the neighboring vertices. Each vertex sets its own label as the 
 * weighted average of the values of its neighbors. The sampling nodes values are in each iteration reset
 * to their real values.  
 * The algorithm can deal with weighted graphs
 * 
 */

object LP_sr {
  
  // Runs LP algorithm on the given dataset and returns the new labels
  def run(g: Graph[Double,Double], S: VertexRDD[Double], real_l: VertexRDD[Double], MAX_ITER: Int, path: String, numPart:Int) = {
    
    // Precompute norm of real signal values
    val norm_x = sqrt(real_l.values.map(x => pow(x,2.0)).reduce(_+_))
    val mses = Array.ofDim[Double](MAX_ITER)
    
    // Keep only signals in S, put the rest to 0.0 (unknown)
    val X = g.outerJoinVertices(S)( (vid,v1,v2) => v2.getOrElse(0.0)).cache
    var lp_l: VertexRDD[Double] = X.vertices.cache
    
    // Loop for MAX_ITER
    var startl = 0.0
    var endl = 0.0
    var k = 0
    while (k < MAX_ITER) {
      // Take iteration time
      startl = System.nanoTime
      
      // Compute new signal values
      lp_l = VertexRDD(X.joinVertices(lp_l)((vid,vd1,vd2)=> vd2).mapTriplets{triplet => 
        (triplet.srcAttr * triplet.attr, triplet.dstAttr * triplet.attr, triplet.attr)
      }.collectEdges(EdgeDirection.Either).mapValues{(vid,vd) => 
        1.0/vd.map(x => x.attr._3).reduce(_+_) * vd.map(x => if (vid == x.srcId) x.attr._2 else x.attr._1).reduce(_+_)
      }
      
      // Keep unchanged the original sample set
      .leftJoin(S)((vid,v1,v2) => v2.getOrElse(v1) ).repartitionAndSortWithinPartitions(new HashPartitioner(numPart))).localCheckpoint
      lp_l.count()
      
      // Compute mse
      mses(k) = sqrt(lp_l.innerZipJoin(real_l)((vid,v1,v2) => pow(v2 - v1,2.0)).values.reduce(_ + _))
      mses(k) = pow(mses(k)/norm_x,2.0)
      println("MSE = " + mses(k).toString())
      
      // Update K
      k = k + 1
      
      // Print iteration time/info
      endl = System.nanoTime
      println("Iteration: " + k.toString() + ", Iter time: " + ((endl-startl)/1000000000.0).toString())
    }
    // Write MSEs  
    val pw = new PrintWriter(new File(path+"MSEs.csv"))
    pw.write("MSEs \n")
    mses.foreach(x => pw.write(x.toString() + "\n"))
    pw.close
    lp_l.repartition(numPart).cache
  }  
  
}
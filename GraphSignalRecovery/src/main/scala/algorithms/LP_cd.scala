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
 * Implementation of the Label Propagation algorithm for community detection. 
 * First the communities in the graph are found and then each community is assigned the
 * most frequent label of the sampled nodes from that community.
 * Weights are not taken into consideration
 * 
 */


object LP_cd {
  
  // Runs label propagation on the given dataset and returns the new labels
  def run(g: Graph[Double,Double], S: VertexRDD[Double], MAX_ITER: Int, numPart:Int) = {
    
    // Keep only signals in S, put the rest to 0.0 (unknown)
    val X = g.outerJoinVertices(S)( (vid,v1,v2) => v2 match {
          case Some(y) => y     // if values in sampling set x(value) = rating
          case None => 0.0      // else x(value) = 0 "unknown rating"
    }).cache
    
    // Compute communities using LPA
    val lp_labels = lib.LabelPropagation.run(X, MAX_ITER).vertices.cache
    println("Communities found: " + lp_labels.values.distinct().count)
    
    // Compute label for each community as most frequent known label (else 0.0, if unknown community label)
    val aux = lp_labels.innerZipJoin(X.vertices)((vid,comvid,label) => (comvid,Map(label -> 1L))).values
    .reduceByKey((map1, map2) =>
    (map1.keySet ++ map2.keySet).map { i =>
      val count1Val = map1.getOrElse(i, 0L)
      val count2Val = map2.getOrElse(i, 0L)
      i -> (count1Val + count2Val)
    }.toMap).mapValues(x => if (x.-(0.0).isEmpty) 0.0 else (x.-(0.0)).maxBy(_._2)._1).cache
    
    // Set all node in the community to selected label
    lp_labels.map(x => x.swap).join(aux).values.cache
  }
  
}
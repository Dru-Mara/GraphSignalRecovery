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
 * Implementation of the Network Lasso method proposed in Hallac et. al. 2015. 
 * This implementation was also used in the paper "RECOVERY CONDITIONS AND SAMPLING STRATEGIES FOR NETWORK LASSO"
 * by A. Mara and A.Jung 2017
 * 
 */

object Nw_lasso {
  
  // Runs Nw_lasso algorithm on the given dataset and returns the new labels
  def run(g: Graph[Double,Double], S: VertexRDD[Double], real_l: VertexRDD[Double], MAX_ITER: Int, path: String, numPart:Int) = {
    
    // Initialize some variables
    val rho = 10.0      // default value in the NW lasso paper rho = 1.0 determines how much we let the variables at each node deviate from their real values
    val lambda = 20.0   // has to be scaled according to the rho value
    val lambda2 = 0.1
    
    // Precompute norm of real signal values
    val norm_x = sqrt(real_l.values.map(x => pow(x,2.0)).reduce(_+_))
    val mses = Array.ofDim[Double](MAX_ITER)
    
    // Keep only signals in S, put the rest to 0.0 (unknown)
    var X = g.outerJoinVertices(S){ (vid,v1,v2) => v2 match {
       case Some(y) => (y,y)  // initialize known node signals to their value (x_k+1,x_k)
       case None => (0.0,0.0) // put rest of signal values to 0 (x_k+1,x_k)
    }}
    .mapTriplets{triplet =>                                                      // initialization z_ij = x_i, z_ji = x_j
       ((triplet.srcAttr._1,triplet.dstAttr._1),(0.0,0.0), lambda*triplet.attr)  // ((z_ij,z_ji),(u_ij,u_ji),lambda*w_ij)
    }.cache
    
    // Loop for MAX_ITER
    var startl = 0.0
    var endl = 0.0
    var k = 0
    while (k < MAX_ITER) {
      // Take iteration time
      startl = System.nanoTime
      
      // Compute new signal values as argmin(ObjFunc)
      // Keep both (x_k+1,x_k) as node signal values
      val vertices = X.collectEdges(EdgeDirection.Either).innerJoin(X.vertices){(vid,vd1,vd2) => 
        val a = vd1.map(x => if (x.srcId == vid) x.attr._1._1 - x.attr._2._1 else x.attr._1._2 - x.attr._2._2).reduce(_+_)
        (a/vd1.length,vd2) // abs(a)? for all i has sum(z_ij+u_ij). If i is src use first elem of tupls ((z,z),(u,u)), if dst use second
        }
      .leftJoin(S){ (vid,vd1,vd2) => vd2 match {
          case Some(y) =>            // OPT problem 1 (x - y) + sum(norm(x-z_ij+u_ij)²)           
            if (abs(y-vd1._1) < 1.0/rho) (y,vd1._2._1) else (y - signum(y-vd1._1)/rho,vd1._2._1)            
          case None =>               // OPT problem 2
            (vd1._1,vd1._2._1) // old x_k+1 is now x_k and avg(z-u) is new x_k+1
        }
      }
      
      // Update variables z_ij, z_ji, u_ij and u_ji
      X = X.joinVertices(vertices)((vid,vd1,vd2) => vd2).mapTriplets{ triplet => 
         // Compute theta as max(aux,0.5) where aux = 1-( lamb*w_ij/(pho*(x_i+u_ij)-(x_j+u_ji)) )
         var theta = 0.5
         val den = (rho * math.abs((triplet.srcAttr._2+triplet.attr._2._1) - (triplet.dstAttr._2+triplet.attr._2._2)) )
         if (den != 0.0) theta = scala.math.max(1.0 - ( triplet.attr._3 / den ), 0.5)
         val zij = theta*(triplet.srcAttr._1 + triplet.attr._2._1) + (1.0-theta)*(triplet.dstAttr._1 + triplet.attr._2._2)
         val zji = (1.0-theta)*(triplet.srcAttr._1 + triplet.attr._2._1) + theta*(triplet.dstAttr._1 + triplet.attr._2._2)
         val uij = triplet.attr._2._1 + (triplet.srcAttr._1 - zij)
         val uji = triplet.attr._2._2 + (triplet.dstAttr._1 - zji)
         ((zij,zji),(uij,uji),triplet.attr._3)  
      }.cache
      
      //X.edges.take(5).foreach(x=> println(x))
      
      // Compute mse
      mses(k) = sqrt(X.vertices.innerZipJoin(real_l)((vid,v1,v2) => pow(v2 - v1._1, 2.0)).values.reduce(_ + _))
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
    X.vertices.mapValues(x => x._1).repartition(numPart).cache
  }
  
}
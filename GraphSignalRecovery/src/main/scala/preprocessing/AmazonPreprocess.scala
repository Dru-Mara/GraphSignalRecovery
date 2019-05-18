package preprocessing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/*
 * Amazon dataset preprocessing tool.
 * The dataset is amazon-metadata with products, frequent co-purchase 
 * and product ratings among other features.
 * The data is cleaned and only the important information (VertId, avg rating, [relatedVertIds])
 * is stored in a new folder "a-meta-clean". 
 */


object MetadataPreprocess {
  
  def main(arg: Array[String]) {
 
    // Set log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Initialize Spark
    val conf = new SparkConf().setAppName("Test").setMaster("local[8]")
    val sc = new SparkContext(conf)
    
    // Variables
    val path = "/home/dru/Escritorio/aaltoWork/Nesterov/data/amazon-meta.txt" //amazon-meta.txt
    val outPath = "/home/dru/Escritorio/aaltoWork/Nesterov/data/a-meta-clean"
    
    val hadoopconf = new Configuration(sc.hadoopConfiguration)
    hadoopconf.set("textinputformat.record.delimiter", "Id:")
    val input = sc.newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopconf)
    val products_raw = input.map { case (_, text) => text.toString}.cache
    println("Products raw:", products_raw.count)
    
    val products = products_raw.map{ line => 
      var asin = """\bASIN\b:\s*(\w+)""".r.findFirstMatchIn(line).map(_ group 1).getOrElse("")
      var similar = """\bsimilar\b:([\s*\w]*)\s*\n""".r.findFirstMatchIn(line).map(_ group 1).getOrElse("")
      var avgrate = """\bavg\b\s\brating\b:\s*([0-9]*\.?[0-9]*)""".r.findFirstMatchIn(line).map(_ group 1).getOrElse("")
      List(asin,avgrate,similar)
    }.filter { array => !array.contains("")}.cache
    println("Products processed:", products.count)
    products.take(5).foreach(x => println(x.mkString(",")))
    
    // Write the output file with the desired data
    products.map( array => array.mkString(",")).saveAsTextFile(outPath)
  }
  
}
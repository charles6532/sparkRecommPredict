package encore.SparkEx

import org.apache.spark._
import org.apache.spark.streaming._

object Stream extends Serializable {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
              .setMaster("spark://master:7077")
              .setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val streamdir = ssc.textFileStream("/home/encore/orders")
    
    val orders = streamdir.map(_.split(","))
    val order = orders.map( data => (data(6), 1L) )    // (B,1) (S,1)
    val bscount = order.reduceByKey( (a,b) => a+b )
    bscount.repartition(1).saveAsTextFiles( "/home/encore/output" )
    ssc.start()
    ssc.awaitTermination()    
  }
}

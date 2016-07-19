package main.scala

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import main.java.Entity.LearningModel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import main.scala.Utils.Timers
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.util.control.Breaks._
import main.scala.Utils.StreamingKNORA
import main.scala.Utils.StreamingModel
import main.scala.Utils.Constant

object StreamingMain {
   def main(args: Array[String]): Unit = {

    //Set Streaming KNORA variables
    var streamingKNORA = new StreamingKNORA()
    streamingKNORA.setNumModel(Constant.num_Models)
    streamingKNORA.setModelType(StreamingModel.HoeffdingTree)
    streamingKNORA.setNumValidate(Constant.num_validate)
    streamingKNORA.setNumNeighbour(Constant.num_neighbour);
    streamingKNORA.setIfIntersect(Constant.intersect);

    
    //Set Spark context variable
//    val sc = new SparkContext(new SparkConf().setAppName("Spark KNORA"))
//    sc.setLocalProperty("spark.shuffle.compress", "true")
//    sc.setLocalProperty("spark.shuffle.spill.compress", "true")
//    sc.setLocalProperty("spark.eventLog.compress", "true")
//    sc.setLocalProperty("spark.broadcast.compress", "true")
//    sc.setLocalProperty("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
//    sc.setLocalProperty("spark.io.compression.snappy.blockSize", "16k")
//    sc.setLocalProperty("spark.rdd.compress", "true")
//    sc.setLocalProperty("spark.driver.memory", "2g")
//    sc.setLocalProperty("spark.eventLog.enabled", "true")  
      
     if (args.length < 2) {
       System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
        System.exit(1)
      }
     
     StreamingExamples.setStreamingLogLevels()

     val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val topicsSet = topics.split(",").toSet
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    kafkaParams += ("auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)
    
    lines.count().print()
    
    streamingKNORA.onTrain(lines)
    
    ssc.start()
    ssc.awaitTermination()
     
   }

   //Transfer into (serial,rowData)
   def convertoinstance(line: String): (Long,String) = {
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line
	  return (serialNum,instString)
  }
   
}
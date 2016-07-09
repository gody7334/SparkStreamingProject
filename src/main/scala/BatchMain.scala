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
import org.apache.spark.rdd.RDD

object BatchMain {
   def main(args: Array[String]): Unit = {

   if (args.length < 11) {
     System.err.println(s"""
      |Usage:  
    var Dataset_HDFS:String = args.apply(0)
    var Dataset_header:String = args.apply(1) 
    var num_Models: Int = args.apply(2).toInt
    var ModelType:String = args.apply(3)
    var num_validate:Int = args.apply(4).toInt
    var num_neighbour:Int = args.apply(5).toInt
    var isIntersect:Boolean = args.apply(6).toBoolean
    var num_train_batch:Int = args.apply(7).toInt
    var num_validate_batch:Int = args.apply(8).toInt
    var num_test_batch:Int = args.apply(9).toInt
    var num_val_test_repartition:Int = args.apply(10).toInt
      """.stripMargin)
      System.exit(1)
    }
     
    var Dataset_HDFS:String = args.apply(0).split(":").apply(1)
    var Dataset_header:String = args.apply(1).split(":").apply(1) 
    var num_Models: Int = args.apply(2).split(":").apply(1).toInt
    var ModelType:String = args.apply(3).split(":").apply(1)
    var num_validate:Int = args.apply(4).split(":").apply(1).toInt
    var num_neighbour:Int = args.apply(5).split(":").apply(1).toInt
    var isIntersect:Boolean = args.apply(6).split(":").apply(1).toBoolean
    var num_train_batch:Int = args.apply(7).split(":").apply(1).toInt
    var num_validate_batch:Int = args.apply(8).split(":").apply(1).toInt
    var num_test_batch:Int = args.apply(9).split(":").apply(1).toInt
    var num_val_test_repartition:Int = args.apply(10).split(":").apply(1).toInt
    
    //Set Streaming KNORA variables
    var streamingKNORA = new StreamingKNORA()
    streamingKNORA.setNumModel(num_Models)
    streamingKNORA.setModelType(ModelType)
    streamingKNORA.setNumValidate(num_validate)
    streamingKNORA.setNumNeighbour(num_neighbour)
    streamingKNORA.setIfIntersect(isIntersect)
    
    //Set Spark context variable
    val sc = new SparkContext(new SparkConf().setAppName("Spark KNORA"))
    StreamingKNORA.setSparkContext(sc)
    sc.setLocalProperty("spark.shuffle.compress", "true")
    sc.setLocalProperty("spark.shuffle.spill.compress", "true")
    sc.setLocalProperty("spark.eventLog.compress", "true")
    sc.setLocalProperty("spark.broadcast.compress", "true")
    sc.setLocalProperty("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
    sc.setLocalProperty("spark.io.compression.snappy.blockSize", "16k")
    sc.setLocalProperty("spark.rdd.compress", "true")
    sc.setLocalProperty("spark.driver.memory", "2g")
    sc.setLocalProperty("spark.eventLog.enabled", "true")
    sc.setLocalProperty("spark.default.parallelism", Constant.num_Models.toString())
      
     var trainData = sc.textFile(Constant.dataset_path)
//     streamingKNORA.onTrain(trainData)
     
     var SD = trainData.map( x => convertoinstance(x) ).cache()
     trainData.unpersist()
     
     var numOfData = SD.count()
     var RDDIndex = 0
     val TrainN = num_train_batch
     val ValidateN = num_validate_batch
     val TestN = num_test_batch
     //TODO: Under Streaming, Need to merge unused data into new data
     breakable { while(RDDIndex < numOfData){
         if(RDDIndex+TrainN > numOfData-1)
           break;
         var train:RDD[String] = null
         new Timers("SD.filter to get train Data, ").time{
         train = SD.filter( x => x._1 >= RDDIndex).filter(x => x._1 < RDDIndex + TrainN).repartition(Constant.num_Models).map(x=>x._2).cache()
         train.count()
         }
         RDDIndex += TrainN
         new Timers("onTrain, ").time{
         streamingKNORA.onTrain(train)
         }
         train.unpersist()
         
         if(RDDIndex+ValidateN > numOfData-1)
           break;
         var validate:RDD[String] = null
         new Timers("SD.filter to get validate Data, ").time{
         validate = SD.filter(x => x._1 >= RDDIndex).filter(x => x._1 < RDDIndex + ValidateN).repartition(Constant.num_Models).map(x=>x._2).cache()
         validate.count()
         }
         RDDIndex += ValidateN
         new Timers("onValidate, ").time{
         streamingKNORA.onValidate(validate)
         }
         validate.unpersist()
         
         if(RDDIndex+TestN > numOfData-1)
           break;
         if(RDDIndex > (TrainN+ValidateN)*(1000/ValidateN)){
           var test:RDD[String] = null
           new Timers("SD.filter to get test Data, ").time{
           test = SD.filter(x => x._1 >= RDDIndex).filter(x => x._1 < RDDIndex + TestN).repartition(Constant.num_Models).map(x=>x._2).cache()
           test.count()
           }
           RDDIndex += TestN
           new Timers("onTest, ").time{
           streamingKNORA.onPredict(test)
           }
           test.unpersist()
         }
       }
     }
     SD.unpersist()
     Thread.sleep(86400000);
          
   }
      
   //Transfer into (serial,rowData)
   def convertoinstance(line: String): (Long,String) = {
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line
	  return (serialNum,instString)
  }
   
}
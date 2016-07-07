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

    //Set Streaming KNORA variables
    var streamingKNORA = new StreamingKNORA()
    streamingKNORA.setNumModel(Constant.num_Models)
    streamingKNORA.setModelType(StreamingModel.HoeffdingTree)
    streamingKNORA.setNumValidate(Constant.num_validate)
    streamingKNORA.setNumNeighbour(Constant.num_neighbour);
    streamingKNORA.setIfIntersect(Constant.intersect);
    
    //Set Spark context variable
    val sc = new SparkContext(new SparkConf().setAppName("Spark KNORA"))
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
     val TrainN = 700000
     val ValidateN = 200000
     val TestN = 100000-1
     //TODO: Under Streaming, Need to merge unused data into new data
     breakable { while(RDDIndex < numOfData-1){
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
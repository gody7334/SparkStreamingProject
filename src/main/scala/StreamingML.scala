package main.scala

import org.apache.spark.streaming.dstream.DStream
import main.java.Entity.LearningModel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import main.scala.Utils.Timers

abstract class StreamingML{
  
  def setInitialStreamingLearningModel(data: String)
  
  def onTrain(data: DStream[String]) 
  
  def onValidate(data: DStream[String]) 
  
  def onPredict(data: DStream[String]) 
  
}

object StreamingMain {
   def main(args: Array[String]): Unit = {
     
    var streamingKNORA = new StreamingKNORA()
    streamingKNORA.setNumModel(100)
    streamingKNORA.setModelType(StreamingKNORA.HoeffdingTree)
    streamingKNORA.setNumValidate(1000)
    streamingKNORA.setNumNeighbour(8);
    streamingKNORA.setIfIntersect(true);
    
    
     val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
     var trainData = sc.textFile("./elecNormNew_train.arff")
     System.out.println(trainData.count())
//     var ar = trainData.collect()
//     System.out.println(trainData.collect())
//     for (i <- 0 until 2){
//        System.out.println(ar.apply(i))
//     }
     
     streamingKNORA.onTrain(trainData)
    
     var predictData = sc.textFile("./elecNormNew_predict.arff")
     System.out.println(predictData.count())
     
     streamingKNORA.onValidate(predictData)
     
     streamingKNORA.onPredict(predictData)
     

    
   }
}
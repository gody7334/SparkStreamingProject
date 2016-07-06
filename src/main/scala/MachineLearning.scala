package main.scala

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

abstract class MachineLearning{
  
  def setInitialStreamingLearningModel(data: String)
  
  def onTrain(data: DStream[String]) 
  
  def onValidate(data: DStream[String]) 
  
  def onPredict(data: DStream[String]) 
  
}
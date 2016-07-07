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
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

object BatchMLlibMain {
   def main(args: Array[String]): Unit = {
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
//    sc.setLocalProperty("spark.default.parallelism", "8")
    
    // Load and parse the data file.
    var data:RDD[LabeledPoint] = null
    new Timers("loadLibSVMFile, ").time{
    data = MLUtils.loadLibSVMFile(sc, "./RRBF_1M_HD.libsvm")
    data.count()
    println("data num of partition, " + data.partitions.size)
    }
    
    // Split the data into training and test sets (30% held out for testing)
    var splits:Array[RDD[LabeledPoint]] = null
    new Timers("randomSplit, ").time{
    splits = data.randomSplit(Array(0.7, 0.3))
    }
    
    var trainingData:RDD[LabeledPoint] = null 
    var testData:RDD[LabeledPoint] = null
    new Timers("repartition, ").time{
    trainingData = splits(0).repartition(16) 
    testData = splits(1).repartition(16)
    trainingData.count()
    testData.count()
    }
    
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 8
    val maxBins = 32
    
    var model:DecisionTreeModel = null
    new Timers("DecisionTree.trainClassifier, ").time{
    model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
      model.numNodes
    }
    
    // Evaluate model on test instances and compute test error
    var labelAndPreds:RDD[(Double, Double)] = null
    new Timers("testData prediction, ").time{
    labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
      }
      labelAndPreds.count()
    }
    
    var testErr: Double = 0.0 
    new Timers("calculate testErr, ").time{
    testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    }
    println("Test Error = " + testErr)
    println("data count = " + data.count())
//    println("Learned classification tree model:\n" + model.toDebugString)
    
    Thread.sleep(86400000);
    
    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    
   }
}
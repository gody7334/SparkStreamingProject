
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import moa.classifiers
import moa.classifiers.trees.HoeffdingTree

/**
 * Test: 
 * two object list map to RDD, 
 * then useing "zip" to combine into 1 keyvalue RDD
 * then map into 1 RDD
 */

object ScalaTest {
  
  def main(args: Array[String]): Unit = {
    
    var listModel = List[LearningModel]()
    var listNum = List[LearningModel]()
    for( x <- 1 to 10){
      val model = new HoeffdingTree();
      var l = new LearningModel(model, x)
      var m = new LearningModel(model, x)
      listModel = l::listModel
      listNum = m::listNum
    }
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    
    val modelrdd = sc.parallelize(listModel)
    val numrdd = sc.parallelize(listNum)
    
    modelrdd.map(x=>new LearningModel(x.learner,x.learner_num + 1))
    
    var num_model = numrdd.zip(modelrdd)
    System.out.println(num_model.collect.mkString(", "))
    
    System.out.println(num_model.map(x=>x._1).collect.mkString(", "))
    
    System.out.println(modelrdd.count())
    modelrdd.collect()
    
  }
}
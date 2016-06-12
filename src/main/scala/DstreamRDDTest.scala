
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
 * $ nc -lk 9999`
 * 1 2 3 4 5 6 7 8 9 10
 */

object DstreamRDDTest {
  
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
    
    @transient val defaults = List("magic" -> 2, "face" -> 5, "dust" -> 7 )
    val defaultRdd = sc.parallelize(defaults)
    
    @transient val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("/tmp/spark")
    
    val lines = ssc.socketTextStream("localhost", 9876, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    
    val words2 = lines.map(_.split(" "))
    
    val words_p = words.repartition(2)
    val num_model_p = num_model.repartition(2)
    
    System.out.println(num_model_p.partitions.size)
    
    System.out.println("")
    words_p.count().print()
    System.out.println("")
    System.out.println(num_model_p.count())
    
    
    val stream_rdd = words_p.transform{x => x.zip(num_model_p)}
    stream_rdd.print()
    
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    val historicCount = wordCount.updateStateByKey[Int]{(newValues: Seq[Int], runningCount: Option[Int]) => 
        Some(newValues.sum + runningCount.getOrElse(0))
    }
    val runningTotal = historicCount.transform{ rdd => rdd.union(defaultRdd)}.reduceByKey( _+_ )
    
    
//    modelrdd.count()
//    modelrdd.collect()
//    defaultRdd.collect()
//    wordCount.print()
//    historicCount.print()
//    runningTotal.print()
    ssc.start()

    
  }
}
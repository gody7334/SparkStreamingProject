
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import moa.classifiers
import moa.classifiers.trees.HoeffdingTree


object DstreamRDD {

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
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    val historicCount = wordCount.updateStateByKey[Int]{(newValues: Seq[Int], runningCount: Option[Int]) => 
        Some(newValues.sum + runningCount.getOrElse(0))
    }
    val runningTotal = historicCount.transform{ rdd => rdd.union(defaultRdd)}.reduceByKey( _+_ )
    
    modelrdd.count()
    modelrdd.collect()
    System.out.println(modelrdd.collect().mkString(", "))
//    defaultRdd.collect()
    wordCount.print()
    historicCount.print()
    runningTotal.print()
//    ssc.start()
}
}
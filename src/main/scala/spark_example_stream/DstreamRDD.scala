import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object DstreamRDD {

  def main(args: Array[String]): Unit = {
    
    var l = new LearningModel(null, 1)
    var listModel = List[LearningModel](l)
    for( x <- 1 to 10){
      var l = new LearningModel(null, 1)
      listModel = l::listModel
    }
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    
    @transient val m = listModel
    val modelrdd = sc.parallelize(m)    
    

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
//    defaultRdd.collect()
    wordCount.print()
    historicCount.print()
    runningTotal.print()
    ssc.start()
}
}
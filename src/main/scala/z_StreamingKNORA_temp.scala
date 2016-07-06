/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main.scala

import scala.reflect.ClassTag

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaSparkContext._
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

import main.java.Entity.LearningModel
import moa.classifiers
import moa.classifiers.trees.HoeffdingTree
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import weka.core.converters.InstanceMaker;
import java.io.IOException;
import weka.core.Instance;
import weka.core.Instances;
import moa.core.InstancesHeader;

/**
 * StreamingKMeansModel extends MLlib's KMeansModel for streaming
 * algorithms, so it can keep track of a continuously updated weight
 * associated with each cluster, and also update the model by
 * doing a single iteration of the standard k-means algorithm.
 *
 * The update algorithm uses the "mini-batch" KMeans rule,
 * generalized to incorporate forgetfullness (i.e. decay).
 * The update rule (for each cluster) is:
 *
 * {{{
 * c_t+1 = [(c_t * n_t * a) + (x_t * m_t)] / [n_t + m_t]
 * n_t+t = n_t * a + m_t
 * }}}
 *
 * Where c_t is the previously estimated centroid for that cluster,
 * n_t is the number of points assigned to it thus far, x_t is the centroid
 * estimated on the current batch, and m_t is the number of points assigned
 * to that centroid in the current batch.
 *
 * The decay factor 'a' scales the contribution of the clusters as estimated thus far,
 * by applying a as a discount weighting on the current point when evaluating
 * new incoming data. If a=1, all batches are weighted equally. If a=0, new centroids
 * are determined entirely by recent data. Lower values correspond to
 * more forgetting.
 *
 * Decay can optionally be specified by a half life and associated
 * time unit. The time unit can either be a batch of data or a single
 * data point. Considering data arrived at time t, the half life h is defined
 * such that at time t + h the discount applied to the data from t is 0.5.
 * The definition remains the same whether the time unit is given
 * as batches or points.
 */

class StreamingKNORAModel (
    var ensembleModels: Array[LearningModel],
    var clusterWeights: Array[Double])
    extends Logging with Serializable 
  {

  /**
   * Perform a k-means update on a batch of data.
   */
  var isInit = false
  
  def update(data: RDD[String], decayFactor: Double, timeUnit: String): StreamingKNORAModel = {
//      System.out.println(data.collect().mkString(", "))
      for (i <- 0 until ensembleModels.length){
        System.out.println(ensembleModels.apply(i).learner_num + ", "+ ensembleModels.apply(i).counter)

      }
      
      var sinsts = data.map(line => StreamingKNORATest.convertoinstance(line)).zipWithIndex().map{case (v,k) => ((k%10)+1,v)}
      
      System.out.println(sinsts.count())
      var co_insts = sinsts.collect()
      
      var im = new InstanceMaker();
	    im.setNoHeaderRowPresent(true);
	    if(data.count()>0){
	      if(isInit == false){
	      im.readHeader(data.collect().apply(0));
	      var m = im.m_structure;
	      m.setClassIndex(m.numAttributes()-1);
	      for (i <- 0 until ensembleModels.length){
          ensembleModels.apply(i).learner.setModelContext(new InstancesHeader(m))
  	      ensembleModels.apply(i).learner.prepareForUse()
        }
	      isInit = true
	      }
	      
//          for (i <- 0 until co_insts.length)
//            System.out.println(co_insts.apply(i)._2.numAttributes())
          
//          for (i <- 0 until ensembleModels.length){
//            if(i < co_insts.length){
//              var ins = co_insts.apply(i)
//    //          ensembleModels.apply(i).learner.trainOnInstance(ins)
//              ensembleModels.apply(i).counter+=1
//            }
//          }
          
          var key_inst = sinsts.groupByKey()
          
          System.out.println("Num of Inst list" + key_inst.count())
          System.out.println("Num of models" + ensembleModels.length)
          
          var listModel_update = key_inst.map(x => updatecounter(x._1, x._2, ensembleModels.clone().toList))
          ensembleModels = listModel_update.collect()
	    }
	    
    this
  }
  
  def updatecounter(key: Long, inst: Iterable[Instance], listModel :  List[LearningModel]): LearningModel = {
    var Index = 0
    var closest = Double.PositiveInfinity

//    listModel(Index).learner.trainOnInstance(inst)
//    listModel(Index).counter+=1
    var instlist = inst.toArray
    for (i <- 0 until listModel.size) {
      if(key.toInt == listModel(i).learner_num){
        for(j <- 0 until inst.size){
          listModel(i).learner.trainOnInstance(instlist.apply(j))
        }
        listModel(i).counter += 1
        Index = i
      }
    }
    listModel(Index)
  }
  
}

/**
 * StreamingKMeans provides methods for configuring a
 * streaming k-means analysis, training the model on streaming,
 * and using the model to make predictions on streaming data.
 * See KMeansModel for details on algorithm and update rules.
 *
 * Use a builder pattern to construct a streaming k-means analysis
 * in an application, like:
 *
 * {{{
 *  val model = new StreamingKMeans()
 *    .setDecayFactor(0.5)
 *    .setK(3)
 *    .setRandomCenters(5, 100.0)
 *    .trainOn(DStream)
 * }}}
 */

class StreamingKNORA_temp (
    var k: Int,
    var decayFactor: Double,
    var timeUnit: String) extends Logging with Serializable {

  def this() = this(2, 1.0, StreamingKNORA_temp.BATCHES)

  protected var model: StreamingKNORAModel = new StreamingKNORAModel(null, null)
  protected var models: Array[LearningModel] = new Array[LearningModel](10)
  
  /**
   * Set the number of clusters.
   */
  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }

  /**
   * Set the forgetfulness of the previous centroids.
   */
  def setDecayFactor(a: Double): this.type = {
    require(a >= 0,
      s"Decay factor must be nonnegative but got ${a}")
    this.decayFactor = a
    this
  }

  /**
   * Set the half life and time unit ("batches" or "points"). If points, then the decay factor
   * is raised to the power of number of new points and if batches, then decay factor will be
   * used as is.
   */
  def setHalfLife(halfLife: Double, timeUnit: String): this.type = {
//    require(halfLife > 0,
//      s"Half life must be positive but got ${halfLife}")
//    if (timeUnit != StreamingKNORA.BATCHES && timeUnit != StreamingKNORA.POINTS) {
//      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
//    }
//    this.decayFactor = math.exp(math.log(0.5) / halfLife)
//    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
//    this.timeUnit = timeUnit
    this
  }

  /**
   * Specify initial centers directly.
   */
  def setInitialKNORAModel(num_models: Int, weight: Double, seed: Long): this.type = {
    require(num_models > 0,
      s"Number of model must be positive but got ${num_models}")
    require(weight >= 0,
      s"Weight for each center must be nonnegative but got ${weight}")
      
//    val random = new XORShiftRandom(seed)
//    val centers = Array.fill(k)(Vectors.dense(Array.fill(dim)(random.nextGaussian())))
    val weights = Array.fill(k)(weight)
    
    for( x <- 1 to 10){
      val model = new HoeffdingTree();
      var l = new LearningModel(model, x, 0)
      models(x-1) = l
    }
   
    model = new StreamingKNORAModel(models, weights)
    this
  }
  


  /**
   * Return the latest model.
   */
  def latestModel(): StreamingKNORAModel = {
    model
  }

  /**
   * Update the clustering model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * checks whether the cluster centers have been initialized,
   * and updates the model using each batch of data from the stream.
   *
   * @param data DStream containing vector data
   */
  def trainOn(data: DStream[String]) {
//    assertInitialized()
    data.foreachRDD { (rdd, time) =>
      model = model.update(rdd, decayFactor, timeUnit)
    }
  }

  /**
   * Java-friendly version of `trainOn`.
   */
  def trainOn(data: JavaDStream[Vector]): Unit = trainOn(data.dstream)

  /**
   * Use the clustering model to make predictions on batches of data from a DStream.
   *
   * @param data DStream containing vector data
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[Vector]): DStream[Int] = {
//    assertInitialized()
//    data.map(model.predict)
    return null
  }

  /**
   * Java-friendly version of `predictOn`.
   */
  def predictOn(data: JavaDStream[Vector]): JavaDStream[java.lang.Integer] = {
    JavaDStream.fromDStream(predictOn(data.dstream).asInstanceOf[DStream[java.lang.Integer]])
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   *
   * @param data DStream containing (key, feature vector) pairs
   * @tparam K key type
   * @return DStream containing the input keys and the predictions as values
   */
  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Int)] = {
//    assertInitialized()
//    data.mapValues(model.predict)
    return null
  }

  /**
   * Java-friendly version of `predictOnValues`.
   */
  def predictOnValues[K](
      data: JavaPairDStream[K, Vector]): JavaPairDStream[K, java.lang.Integer] = {
//    implicit val tag = fakeClassTag[K]
//    JavaPairDStream.fromPairDStream(
//      predictOnValues(data.dstream).asInstanceOf[DStream[(K, java.lang.Integer)]])
    return null
  }

  /** Check whether cluster centers have been initialized. */
  private[this] def assertInitialized(): Unit = {
//    if (model.clusterCenters == null) {
//      throw new IllegalStateException(
//        "Initial cluster centers must be set before starting predictions")
//    }
  }
}

private object StreamingKNORA_temp {
  final val BATCHES = "batches"
  final val POINTS = "points"
}

object StreamingKNORATest {
   def main(args: Array[String]): Unit = {
     var knora = new StreamingKNORA_temp()
     knora.setInitialKNORAModel(10, 0.1, 1)
     
     val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
     val defaults = List("magic" -> 2, "face" -> 5, "dust" -> 7 )
     val defaultRdd = sc.parallelize(defaults)
     defaultRdd.collect()
     
     var lines2 = sc.textFile("./covtype4.data")
     System.out.println(lines2.count())
     var ar = lines2.collect()
     System.out.println(lines2.collect())
     for (i <- 0 until 10){
        System.out.println(ar.apply(i))
     }
     
     var insts = lines2.map(line => convertoinstance(line))
     System.out.println(insts.count())
     var ar2 = insts.collect()
     System.out.println(insts.collect())
     
     System.out.println(ar2.size)
     
     for (j <- 0 until 10){
        System.out.println(ar2.apply(j).toDoubleArray())
     }
     
     
     
     val ssc = new StreamingContext(sc, Seconds(10))
     ssc.checkpoint("/tmp/spark")
    
     val lines = ssc.socketTextStream("localhost", 9876, StorageLevel.MEMORY_AND_DISK_SER)
     knora.trainOn(lines)
     
     
//     var sinsts = lines.map(line => convertoinstance(line))
//     sinsts.count().print()
//     val words = lines.flatMap(_.split(" "))
//     
//     knora.trainOn(words)
//     
//     words.print()
     ssc.start()

   }
   
   def convertoinstance(line: String): Instance = {
     var im = new InstanceMaker()
     im.setNoHeaderRowPresent(true)
     
//    	try{
    	  im.readHeader(line);
    	  
    	  
//    	}catch{
//    	  case a:IOException => println(a);
//    	  case b:NullPointerException => println(b);
//    	  case _:Throwable => println("Throwable");
//    	}
    	
//    	if(im != null){
    	  var m = im.m_structure;
    	  m.setClassIndex(m.numAttributes()-1)
    	  var inst = im.convertToInstance(line, m)
    	  
    	  return inst
//    	}
//    	return null
//      return null
   }
   
}

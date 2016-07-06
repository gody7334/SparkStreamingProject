package main.scala.Utils

import org.apache.spark.streaming.dstream.DStream
import main.java.Entity.LearningModel
import main.java.Entity.PredictResult
import main.java.Entity.ValidateInstance
import main.java.Utils.MajorityVote
import moa.classifiers.trees.HoeffdingTree
import weka.core.converters.InstanceMaker
import org.apache.spark.rdd.RDD
import weka.core.Instances
import weka.core.Instance
import moa.streams.ArffFileReader
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import main.java.Utils.KNORA;
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import main.scala.Utils.Constant
import main.scala.MachineLearning
import main.scala.Utils.Timers

class StreamingKNORA extends MachineLearning with Serializable{
  
  var models: Array[LearningModel] = _
  var ValidateList: Array[ValidateInstance] = _
  var validateList_index = 0
  var num_Models: Int = _
  var num_validate: Int = _
  var ModelType: String = _
  var isInitial: Boolean = false
  var instances: Instances = _
  var instanceMaker: InstanceMaker = _
  var num_classes: Int = _
  var num_neighbour: Int = _
  var intersect: Boolean = _
  var knora: KNORA = _
  var instance_header_path = "./File/RRBF_1M_H.arff"
  var num_testInstance: Int = 0
  var num_testCorrectInst: Int = 0
  
    
  def convertoinstance(line: String): (Long,Instance) = {
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line.substring(line.indexOf(",")+1)
	  var inst = instanceMaker.convertToInstance(instString, instances)
	  return (serialNum,inst)
  }
  
  def setNumModel(num_Models: Int) = {
    this.num_Models = num_Models
  }
  
  def setModelType(ModelType: String) = {
    this.ModelType = ModelType
  }
  
  def setNumClasses(num_classes: Int) = {
    this.num_classes = num_classes
  }
  
  def setNumValidate(num_validate: Int) = {
    this.num_validate = num_validate
  }
  
  def setNumNeighbour(num_neighbour: Int) = {
    this.num_neighbour = num_neighbour
  }
  
  def setIfIntersect(intersect: Boolean) = {
    this.intersect = intersect
  }
  
  def onTrain(data: RDD[String]) = {
    TrainOnInstances(data)
  }
  
  def onTrain(data: DStream[String]) = { 
    data.foreachRDD { rdd => TrainOnInstances(rdd)}
  }
  
  def onValidate(data: RDD[String]) = {
    ValidateOnInstances_KNORA(data)
  } 
  
  def onValidate(data: DStream[String]) = {
    
  } 
  
  def onPredict(data: RDD[String]) = {
//    PredictOnInstances_MV(data)
    PredictOnInstances_KNORA(data)
  }
  
  def onPredict(data: DStream[String]) = {
    
  }
  
  def setInitialStreamingLearningModel( instance_header_path: String) = {
    
    var arff = new ArffFileReader()
    val whereami = System.getProperty("user.dir")
    System.out.println(whereami)
    
    var arffStream = arff.ArffRead(instance_header_path)
    instanceMaker = new InstanceMaker()
    instances = arffStream.instances
    
    this.models = new Array[LearningModel](num_Models)
    this.ValidateList = new Array[ValidateInstance](num_validate)
    this.knora = new KNORA(num_neighbour, intersect)
    
    for( i <- 1 to num_Models){
      var LM = initialModel()
      LM.learner.setModelContext(arffStream.getHeader())
      LM.learner.prepareForUse()
      LM.learner_num = i
      models.update(i-1, LM)
    }
    
    println("Number of models: " +models.length)
    System.out.println("Number of classes: " + instances.numClasses())
    System.out.println("Inintial Learning Model Done!!!")
  }
  
  def initialModel(): LearningModel = {
    
      ModelType match{
      case StreamingModel.HoeffdingTree =>{ 
        var model = new HoeffdingTree()
        var LM = new LearningModel(model, 0)
        return LM
      }
    }
  }
  
  def TrainOnInstances(data: RDD[String]) = {
    if(isInitial == false){
      setInitialStreamingLearningModel(instance_header_path)
      isInitial = true
    }
    
//    var key_instance:RDD[(Long, (Long, Instance))] = null
    var serial_instance:RDD[(Long, Instance)] = null
    Timers.time{
    //covert sting into instance and add key(learner_number) to each instance
//    key_instance = data.map(line => convertoinstance(line)).zipWithIndex().map{case (v,k) => ((k%num_Models)+1,v)}
      serial_instance = data.map(line => convertoinstance(line)).repartition(Constant.num_Models)
    }

    var group_instances:RDD[(Long, Iterable[(Long, Instance)])] = null
//    Timers.time{
//    //group instances by key(learner_number)
//    group_instances = key_instance.groupByKey()
//    }
    
    var update_Models:RDD[LearningModel] = null
    Timers.time{
    //train models on group instances
//    update_Models = group_instances.map(x => TrainOnInstancesTransform(x._1, x._2, models))
      update_Models = serial_instance.mapPartitionsWithIndex((index, x) => TrainOnInstancesTransform(index, x, models))
    }
    
    Timers.time{
    //update models
    models = update_Models.collect()
    println(models.length)
    }
    
    System.out.println("Train Learning Model Done!!!")
  }
  
  def TrainOnInstancesTransform(key: Long, inst: Iterator[(Long,Instance)], models : Array[LearningModel]): Iterator[LearningModel] = {
    //find corresponding learning model
    var index = 0;
    for(i <- 0 until models.length){
      if(models.apply(i).learner_num == key)
        index = i
    }
    
    //sort training dataset
//    var num_inst = inst.toList.sortBy(_._1)
    var num_inst = inst.toList
    
    //Train on dataset
    for(i <- 0 until num_inst.length){
      models(index).learner.trainOnInstance(num_inst(i)._2)
    }
    var ItorModels: Iterator[LearningModel] = Iterator(models(index))
    return ItorModels
  }
  
  def ValidateOnInstances_KNORA(data: RDD[String]) = {
    //covert sting into instance
    var insts = data.map(line => convertoinstance(line))
    
    //for each instance, validate on every model and record the model which correct predict the instance
    var validateresult = insts.map(inst => ValidateOnInstances_KNORA_Transformation(inst, models))
    
    var VRArray = validateresult.collect()
    
    //Assign validate data into ValidateList
    for(i <- 0 until VRArray.length){
      ValidateList.update(validateList_index, VRArray.apply(i))
      validateList_index+=1
      if(validateList_index>=ValidateList.length){
        validateList_index = 0
      }
    }
    System.out.println("Validate Learning Model Done!!!")
  }
  
  def ValidateOnInstances_KNORA_Transformation(inst: (Long,Instance), models: Array[LearningModel]): ValidateInstance = {
    var v = new ValidateInstance();
    v.Validate_Inst = inst._2;
    for(i <- 0 until models.length){
		  if(models.apply(i).learner.correctlyClassifies(inst._2))
				   v.list_positive_learner_num.add(models.apply(i).learner_num);
		}
    return v
  }
  
  def PredictOnInstances_MV(data: RDD[String]) = {
    //covert sting into instance
    var insts = data.map(line => convertoinstance(line))
    var num_insts = insts.count()
    
    //for each instance, predict on every model and do majority vote
    var result = insts.map(inst => PredictOnInstances_MV_Transformation(inst, models))
    
    //MLlib statistic function
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    val mean = summary.mean
    
    //Calculate Accuracy
    this.num_testInstance += num_insts.toInt
    this.num_testCorrectInst += (mean.apply(0)*num_insts).toInt
    println(num_testCorrectInst.toDouble / num_testInstance.toDouble)    
    System.out.println("Predict MV Learning Model Done!!!")
    
    
  }
  def PredictOnInstances_MV_Transformation(inst: (Long,Instance), models: Array[LearningModel]): Vector = {
    
    //Get predict class (getVotesForInstance)
    var PredictResultArray = new Array[PredictResult](models.length)
    for(i <- 0 until models.length){
      var result = models.apply(i).learner.getVotesForInstance(inst._2)
      var predict_result = new PredictResult(models.apply(i).learner_num, result);
      PredictResultArray.update(i, predict_result)
    }
    
    //Majority vote
    var predictClassIdx = MajorityVote.Vote(PredictResultArray, instances.numClasses())
    var trueClassIdx = inst._2.classValue().toInt
    if(predictClassIdx == trueClassIdx){
			 return Vectors.dense(1.0)
		}
    else{
       return Vectors.dense(0.0)
    }
  }
  
  def PredictOnInstances_KNORA(data: RDD[String]) = {
    //covert sting into instance
    var insts = data.map(line => convertoinstance(line))
    var num_insts = insts.count()
    
    //for each instance, predict on every model and do KNORA & majority vote
    var result = insts.map(inst => PredictOnInstances_KNORA_Transformation(inst, models))
    
    //MLlib statistic function
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    val mean = summary.mean
    
    //Calculate Accuracy
    this.num_testInstance += num_insts.toInt
    this.num_testCorrectInst += (mean.apply(0)*num_insts).toInt
    println(num_testCorrectInst.toDouble / num_testInstance.toDouble)    
    System.out.println("Predict KNORA Learning Model Done!!!")
    
  }
    
  def PredictOnInstances_KNORA_Transformation(inst: (Long,Instance), models: Array[LearningModel]): Vector = {
    
    //Get predict class (getVotesForInstance)
    var PredictResultArray = new Array[PredictResult](models.length)
    for(i <- 0 until models.length){
      var result = models.apply(i).learner.getVotesForInstance(inst._2)
      var predict_result = new PredictResult(models.apply(i).learner_num, result);
      PredictResultArray.update(i, predict_result)
    }
    
    //Find KNN instances' classifiers which correctly predict the instance(inst._2)
    var Intesect_classifier_number =knora.findKNNValidateInstances(ValidateList, inst._2, instances);
    
    //Find Predict_Result which: learner number = intesect_classifier_number
    var interset_PredictResult_Array = new Array[PredictResult](Intesect_classifier_number.length)
    for(i <- 0 until Intesect_classifier_number.length){
      for(j <- 0 until PredictResultArray.length){
				 if(PredictResultArray.apply(j).learner_num ==  Intesect_classifier_number.apply(i)){
					 interset_PredictResult_Array.update(i, PredictResultArray.apply(j))
			   }
      }
    }
    
    //If no intesect, use all classifier (MV)
    if(Intesect_classifier_number.length == 0){
      interset_PredictResult_Array = PredictResultArray
    }
    
    //Do Majority vote on intesect classifier
    var predictClassIdx = MajorityVote.Vote(interset_PredictResult_Array, instances.numClasses())
    var trueClassIdx = inst._2.classValue().toInt
    if(predictClassIdx == trueClassIdx){
			 return Vectors.dense(1.0)
		}
    else{
       return Vectors.dense(0.0)
    }
  }
  
}
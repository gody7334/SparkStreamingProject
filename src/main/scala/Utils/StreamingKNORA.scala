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
import com.madhukaraphatak.sizeof.SizeEstimator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast

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
  
  def convertoinstance(line: String): (Long,Instance) = {
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line.substring(line.indexOf(",")+1)
	  var inst = instanceMaker.convertToInstance(instString, instances)
	  return (serialNum,inst)
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
    
    //!! learner_number 1:n
    for( i <- 1 to num_Models){
      var LM = initialModel()
      LM.learner.setModelContext(arffStream.getHeader())
      LM.learner.prepareForUse()
      LM.learner_num = i
//      println(SizeEstimator.estimate(LM))
//      println(SizeEstimator.estimate("a"))
      models.update(i-1, LM)
    }
    
    StreamingKNORA.broadcastModels = StreamingKNORA.sc.broadcast(models)
    StreamingKNORA.broadcastValidateList = StreamingKNORA.sc.broadcast(ValidateList)
    
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
   
    println("data partition size: " + data.partitions.size)
        
    var update_Models:RDD[LearningModel] = null
    new Timers("onTrain: mapPartitionsWithIndex, ").time{
    //train models on instances on Each partition
      update_Models = data.mapPartitionsWithIndex((index, x) => TrainOnInstancesTransform(index, x)).cache()
      println("update_Models partition size: " + update_Models.partitions.size)
      update_Models.count()
    }
    
    new Timers("onTrain: update_Models.collect, ").time{
    //update models
    models = update_Models.collect()
    }
    
    StreamingKNORA.broadcastModels.destroy()
    StreamingKNORA.broadcastModels = StreamingKNORA.sc.broadcast(models)
    
    System.out.println("Train Learning Model Done!!!")
    update_Models.unpersist()
  }
  
  def TrainOnInstancesTransform(key: Long, line: Iterator[String]): Iterator[LearningModel] = {
    //find corresponding learning model
    var ML:LearningModel = null
    var num_models = StreamingKNORA.broadcastModels.value.length
    for(i <- 0 until num_models){
       //!! learner_number=1:n, key=0:n-1
      var MLtemp =StreamingKNORA.broadcastModels.value.apply(i) 
      if(MLtemp.learner_num-1 == key)
        ML = MLtemp
    }
    
    //Convert to Instance and Train on instance
    //Utilize the CPU cache, extremely fast
    while(line.hasNext){
      var strLine = line.next()
      var common = strLine.indexOf(',')
      var serialNum = strLine.substring(0, common).toLong
      var instString = strLine.substring(common+1)
  	  var inst = instanceMaker.convertToInstance(instString, instances)
  	  ML.learner.trainOnInstance(inst)
    }
    
    var ItorModels: Iterator[LearningModel] = Iterator(ML)
    return ItorModels
  }
  
  def ValidateOnInstances_KNORA(data: RDD[String]) = {
    
    //for each instance, validate on every model and record the model which correct predict the instance
    var validateresult:RDD[ValidateInstance] = null
    new Timers("onValidate: OnInstances_KNORA_Transformation, ").time{
    validateresult = data.map(line => ValidateOnInstances_KNORA_Transformation(line)).cache()
    validateresult.count()
    }
    
    var VRArray:Array[ValidateInstance] = null
    new Timers("onValidate: validateresult.collect, ").time{
    VRArray = validateresult.collect()
    }
    
    //Assign validate data into ValidateList
    new Timers("onValidate: update ValidateList, ").time{
    for(i <- 0 until VRArray.length){
      ValidateList.update(validateList_index, VRArray.apply(i))
      validateList_index+=1
      if(validateList_index>=ValidateList.length){
        validateList_index = 0
      }
    }
    }
    
    StreamingKNORA.broadcastValidateList.destroy()
    StreamingKNORA.broadcastValidateList = StreamingKNORA.sc.broadcast(ValidateList)
    
    System.out.println("Validate Learning Model Done!!!")
    validateresult.unpersist()
  }
  
  def ValidateOnInstances_KNORA_Transformation(line:String): ValidateInstance = {
  
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line.substring(common+1)
	  var inst = instanceMaker.convertToInstance(instString, instances)
    
    var v = new ValidateInstance();
    v.Validate_Inst = inst;
    var num_model = StreamingKNORA.broadcastModels.value.length
    for(i <- 0 until num_model){
      var model = StreamingKNORA.broadcastModels.value.apply(i)
		  if(model.learner.correctlyClassifies(inst))
				   v.list_positive_learner_num.add(model.learner_num);
		}
    return v
  }
  
  def PredictOnInstances_MV(data: RDD[String]) = {
    //covert sting into instance
    var insts:RDD[(Long, Instance)] = null
    new Timers("onPredict_MV: convertoinstance, ").time{
    insts = data.map(line => convertoinstance(line))
    }
    var num_insts = insts.count()
    
    //for each instance, predict on every model and do majority vote
    var result:RDD[Vector] = null
    new Timers("onPredict_MV: OnInstances_MV_Transformation, ").time{
    var result = insts.map(inst => PredictOnInstances_MV_Transformation(inst, models))
    }
    
    //MLlib statistic function
    var mean:Vector = null
    new Timers("onPredict_MV: Statistics.colStats, ").time{
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    mean = summary.mean
    }
    
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
    
    var num_insts = data.count()
    
    //for each instance, predict on every model and do KNORA & majority vote
    var result:RDD[Vector] = null
    new Timers("onPredict_KNORA: OnInstances_KNORA_Transformation, ").time{
    result = data.map(line => PredictOnInstances_KNORA_Transformation(line)).cache()
    result.count()
    }
       
    //MLlib statistic function
    var mean:Vector = null
    new Timers("onPredict_KNORA: Statistics.colStats, ").time{
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    mean = summary.mean
    }
    
    //Calculate Accuracy
    this.num_testInstance += num_insts.toInt
    this.num_testCorrectInst += (mean.apply(0)*num_insts).toInt
    println(num_testCorrectInst.toDouble / num_testInstance.toDouble)    
    System.out.println("Predict KNORA Learning Model Done!!!")
//    insts.unpersist()
    result.unpersist()
  }
    
  def PredictOnInstances_KNORA_Transformation(line:String): Vector = {
    
    var common = line.indexOf(',')
    var serialNum = line.substring(0, common).toLong
    var instString = line.substring(common+1)
	  var inst = instanceMaker.convertToInstance(instString, instances)
    
    //Get predict class (getVotesForInstance)
	  
    var num_model = StreamingKNORA.broadcastModels.value.length
    var PredictResultArray = new Array[PredictResult](num_model)
//    new Timers("PredictResultArray, ").time{
    for(i <- 0 until num_model){
      var ML = StreamingKNORA.broadcastModels.value.apply(i)
      var result = ML.learner.getVotesForInstance(inst)
      var predict_result = new PredictResult(ML.learner_num, result);
      PredictResultArray.update(i, predict_result)
    }
//    }
    
    //Find KNN instances' classifiers which correctly predict the instance(inst._2)
    var Intesect_classifier_number:Array[Integer]=null
//    new Timers("KNN, ").time{
    Intesect_classifier_number =knora.findKNNValidateInstances(StreamingKNORA.broadcastValidateList.value, inst, instances);
//    }
    
    //Find Predict_Result which: learner number = intesect_classifier_number
    var num_interset = Intesect_classifier_number.length
    var interset_PredictResult_Array = new Array[PredictResult](num_interset)
//    new Timers("find interset, ").time{
    var num_PredicResult = PredictResultArray.length
    for(i <- 0 until num_interset){
      for(j <- 0 until num_PredicResult){
				 if(PredictResultArray.apply(j).learner_num ==  Intesect_classifier_number.apply(i)){
					 interset_PredictResult_Array.update(i, PredictResultArray.apply(j))
			   }
      }
    }
//    }
    
    //If no intesect, use all classifier (MV)
    if(num_interset == 0){
      interset_PredictResult_Array = PredictResultArray
    }
    
    //Do Majority vote on intesect classifier
    var predictClassIdx:Int = 0
//    new Timers("Majority vote, ").time{
    predictClassIdx = MajorityVote.Vote(interset_PredictResult_Array, instances.numClasses())
//    }
    var trueClassIdx = inst.classValue().toInt
    if(predictClassIdx == trueClassIdx){
			 return Vectors.dense(1.0)
		}
    else{
       return Vectors.dense(0.0)
    }
  }
}

object StreamingKNORA {
  var sc: SparkContext = _
  var broadcastModels: Broadcast[Array[LearningModel]] = _
  var broadcastValidateList: Broadcast[Array[ValidateInstance]] = _
  
  def setSparkContext(sc: SparkContext) = {
    this.sc = sc
  }
}
package main.scala

import org.apache.spark.streaming.dstream.DStream
import main.java.Entity.LearningModel
import main.java.Entity.PredictResult
import main.java.Entity.ValidateInstance
import main.java.Utils.MajorityVote
import moa.classifiers.trees.HoeffdingTree
import moa.classifiers.Classifier
import weka.core.converters.InstanceMaker;
import moa.core.InstancesHeader;
import org.apache.spark.rdd.RDD
import weka.core.Instances;
import weka.core.Instance;
import moa.streams.ArffFileReader
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import main.scala.Utils.Timers;
import main.java.Utils.KNORA;

object StreamingKNORA extends Enumeration {
   val HoeffdingTree = "HoeffdingTree"
} 

class StreamingKNORA extends StreamingML with Serializable{
  
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
  
    
  def convertoinstance(line: String): Instance = {
	  var inst = instanceMaker.convertToInstance(line, instances)
	  return inst
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
      case StreamingKNORA.HoeffdingTree =>{ 
        var model = new HoeffdingTree()
        var LM = new LearningModel(model, 0)
        return LM
      }
    }
      
  }
  
  def TrainOnInstances(data: RDD[String]) = {
    if(isInitial == false){
      setInitialStreamingLearningModel("./File/elecNormNew_header.arff")
      isInitial = true
    }

    /*
     * iteration test
    var insts = data.map(line => convertoinstance(line))
    var instsarray = insts.collect()
    
    for(i <- 0 until instsarray.length){
     models.apply(0).learner.trainOnInstance(instsarray.apply(i))
     models.apply(0).counter+=1
    }*/
    
    
    //covert sting into instance and add key(learner_number) to each instance
    var key_instance = data.map(line => convertoinstance(line)).zipWithIndex().map{case (v,k) => ((k%num_Models)+1,v)}

    //group instances by key(learner_number)
    var group_instances = key_instance.groupByKey()
    
    //train models on group instances
    var update_Models = group_instances.map(x => TrainOnInstancesTransform(x._1, x._2, models))
    
    //update models
    models = update_Models.collect()
    
    System.out.println("Train Learning Model Done!!!")
    System.out.println(data.count())
  }
  
  def TrainOnInstancesTransform(key: Long, inst: Iterable[Instance], models : Array[LearningModel]): LearningModel = {
    var index = 0;
    for(i <- 0 until models.length){
      if(models.apply(i).learner_num == key)
        index = i
    }
    
    var instances = inst.toArray
    for(i <- 0 until instances.length){
      models(index).learner.trainOnInstance(instances.apply(i))
//      models(index).counter+=1
    }
    return models(index)  
  }
  
  def onTrain(data: RDD[String]) = {
    TrainOnInstances(data)
  }
  
  def onTrain(data: DStream[String]) = { 
    data.foreachRDD { rdd => TrainOnInstances(rdd)}
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
    
    println(validateresult.count())
    println(ValidateList.length)
    System.out.println("Validate Learning Model Done!!!")
  }
  
  def ValidateOnInstances_KNORA_Transformation(inst: Instance, models: Array[LearningModel]): ValidateInstance = {
    
    var v = new ValidateInstance();
    v.Validate_Inst = inst;
    for(i <- 0 until models.length){
			 var result = models.apply(i).learner.getVotesForInstance(inst)
			 
		if(models.apply(i).learner.correctlyClassifies(inst))
				 v.list_positive_learner_num.add(models.apply(i).learner_num);
		}
    
    return v
  }
  
  def onValidate(data: RDD[String]) = {
    ValidateOnInstances_KNORA(data)
  } 
  
  def onValidate(data: DStream[String]) = {
    
  } 
  
  def PredictOnInstances_MV(data: RDD[String]) = {
    //covert sting into instance
    var insts = data.map(line => convertoinstance(line))
    
    /*
     * Iteration Test
    var instsarray = insts.collect()
    var numberSamplesCorrect = 0
    var numberTestSamples = 0
    for(i <- 0 until instsarray.length){
     if(models.apply(0).learner.correctlyClassifies(instsarray.apply(i))){
				 numberSamplesCorrect+=1;
			 }
     numberTestSamples+=1;
    }
    println("Accuracy: " + (numberSamplesCorrect.toDouble / numberTestSamples.toDouble))
    */

    
    //for each instance, predict on every model and do majority vote
    var result = insts.map(inst => PredictOnInstances_MV_Transformation(inst, models))
    
    //MLlib statistic function
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    println(summary.mean)
    
    System.out.println("Predict MV Learning Model Done!!!")
    
    
  }
  def PredictOnInstances_MV_Transformation(inst: Instance, models: Array[LearningModel]): Vector = {
    
    var PredictResultArray = new Array[PredictResult](models.length)
    for(i <- 0 until models.length){
      var result = models.apply(i).learner.getVotesForInstance(inst)
      var predict_result = new PredictResult(models.apply(i).learner_num, result);
      PredictResultArray.update(i, predict_result)
    }
    var predictClassIdx = MajorityVote.Vote(PredictResultArray, instances.numClasses())
    var trueClassIdx = inst.classValue().toInt
    if(predictClassIdx == trueClassIdx){
			 return Vectors.dense(1.0)
		}
    else{
       return Vectors.dense(0.0)
    }
  }
  
  def PredictOnInstances_KNORA(data: RDD[String]) = {
    //covert sting into instance
//    var inst = convertoinstance(data.first())
//    var pre = PredictOnInstances_KNORA_Transformation(inst, models)
    
    var insts = data.map(line => convertoinstance(line))
    
    //for each instance, predict on every model and do KNORA & majority vote
    var result = insts.map(inst => PredictOnInstances_KNORA_Transformation(inst, models))
    
    //MLlib statistic function
    val summary: MultivariateStatisticalSummary = Statistics.colStats(result)
    println(summary.mean)
    
    System.out.println("Predict KNORA Learning Model Done!!!")
    
  }
    
  def PredictOnInstances_KNORA_Transformation(inst: Instance, models: Array[LearningModel]): Vector = {
    
//    println("num of learning models: " + models.length)
    var PredictResultArray = new Array[PredictResult](models.length)
    for(i <- 0 until models.length){
//      println("learner number: " + models.apply(i).learner_num)
      var result = models.apply(i).learner.getVotesForInstance(inst)
      var predict_result = new PredictResult(models.apply(i).learner_num, result);
      PredictResultArray.update(i, predict_result)
    }
    
    var Intesect_result =knora.findKNNValidateInstances(ValidateList, inst, instances);
    
//    for(i <- 0 until Intesect_result.length){
//      println("intersect learner number: " + Intesect_result.apply(i))
//    }
    
    var interset_PredictResult_Array = new Array[PredictResult](Intesect_result.length)
    for(i <- 0 until Intesect_result.length){
      for(j <- 0 until PredictResultArray.length){
				 if(PredictResultArray.apply(j).learner_num ==  Intesect_result.apply(i)){
					 interset_PredictResult_Array.update(i, PredictResultArray.apply(j))
			   }
      }
    }
//      
//      var prediction = PredictResultArray.find { x => x.learner_num == Intesect_result.apply(i) }
//      if (!prediction.isEmpty){
//        interset_PredictResult_Array.update(i, prediction.get)
//      }
//    }
    
    if(Intesect_result.length == 0){
      interset_PredictResult_Array = PredictResultArray
    }
    
    var predictClassIdx = MajorityVote.Vote(interset_PredictResult_Array, instances.numClasses())
    var trueClassIdx = inst.classValue().toInt
    if(predictClassIdx == trueClassIdx){
			 return Vectors.dense(1.0)
		}
    else{
       return Vectors.dense(0.0)
    }
  }
  
  def onPredict(data: RDD[String]) = {
//    PredictOnInstances_MV(data)
    PredictOnInstances_KNORA(data)
  }
  
  def onPredict(data: DStream[String]) = {}
  
}
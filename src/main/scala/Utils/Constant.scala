package main.scala.Utils

object Constant {
  
  //KNORA variables
  var num_Models: Int = 8
  var num_validate: Int = 1000
  var ModelType: String = StreamingModel.HoeffdingTree
  var num_neighbour: Int = 8
  var intersect: Boolean = true
//  var instance_header_path = "./File/elecNormNew_header.arff"
  var instance_header_path = "./File/RRBF_1M_H.arff"  
  var dataset_path = "./RRBF_1M_SD.arff"
  
}

object StreamingModel extends Enumeration {
   val HoeffdingTree = "HoeffdingTree"
} 
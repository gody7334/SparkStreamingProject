
object ScalaTest {
  
  def main(args: Array[String]): Unit = {
    
    var l = new LearningModel(null, 1)
    var listModel = List[LearningModel](l)
    for( x <- 1 to 10){
      var l = new LearningModel(null, 1)
      listModel = l::listModel
    }
    val m = new LearningModel(null, 1)
    
  }
}
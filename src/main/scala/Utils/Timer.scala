package main.scala.Utils

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

object Timers{
  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1e9 + " s")
    result
  }
}

class Timers(var message: String){
  var logger = LogManager.getLogger("myLogger")
  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println( message + " Elapsed time: " + (t1 - t0)/1e9 + " s")
    logger.info(message + " Elapsed time: " + (t1 - t0)/1e9 + " s")
    result
  }
}
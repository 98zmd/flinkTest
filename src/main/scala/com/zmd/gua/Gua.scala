package com.zmd.gua

import scala.util.Random

object Gua {
  def main(args: Array[String]): Unit = {

    val result: Array[Int] = new Array(6)
    var eachRes = 0
    for(j <- 0 to 5){
      val init = 50
      val nochange = 1
      var startGua: Int = init - 1
      for(i <- 1 to 3){
        val randomSplit: Int = Random.nextInt(startGua)
        val partOne: Int = randomSplit -nochange
        val partTwo: Int = startGua - randomSplit - 1
        val reduce: Int = (if(partOne%4 == 0) 4 else partOne%4 ) + (if(partTwo%4 == 0) 4 else partTwo%4 )
        eachRes = (if(partOne%4 == 0) partOne/4-1 else partOne/4) + (if(partTwo%4 == 0) partTwo/4-1 else partTwo/4)
        startGua = startGua - reduce - nochange
        result(j) = eachRes
      }

    }
    for(x <- 0 to 5){
      println(matchgraph(result(x)))
    }

    println("change:")
    for(x <- 0 to 5){
      if(result(x) == 6){
        println(matchgraph(9))
      }else if(result(x) == 9){
        println(matchgraph(6))
      }else{
        println(matchgraph(result(x)))
      }


    }
    // 7 8不变  6 9 变
    /*
     *  6 - -
     *  7 ---
     *  8 - -
     *  9 ---
     */
  }
  def matchgraph(x: Int):String = {
    x match {
      case 6 => "- -"
      case 7 => "---"
      case 8 => "- -"
      case 9 => "---"
    }
  }
}

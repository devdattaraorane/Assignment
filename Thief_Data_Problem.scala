package Assignments

import scala.io.Source

object Thief_Data_Problem extends App {

    val source = Source.fromFile("C:\\Users\\d.raorane\\Downloads\\thief_d.txt")

    try{
      source.getLines().foreach{
        line =>
          println(minFlipes(line.trim))
      }
    }
    finally {
      source.close()
    }

    def minFlipes(str: String):Int = {
      var expected = '1'
      var count = 0

      for(ch <- str){
        if(ch != expected){
          count +=1
          expected = if(expected == '1') '0' else '1'
        }
      }

      count
    }
}

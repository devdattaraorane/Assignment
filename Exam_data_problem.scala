package Assignments

import scala.io.Source

object Exam_data_problem extends App{
  val source = Source.fromFile("C:\\Users\\d.raorane\\Downloads\\data.txt")

   try {
    source.getLines().foreach {
      line =>
      val Array(k, l, m) = line.split(",").map(_.trim.toInt)
      println(if (k * l <= m) "YES" else "NO")
    }
   }
  finally {
    source.close()
  }
}

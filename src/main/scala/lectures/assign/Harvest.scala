package lectures.assign

import java.time.LocalDate
import scala.io.Source

object Harvest extends App {

  case class Entry(gatherer: String, date: LocalDate, fruit: String, amount: Double, earning: Double)

  //load the prices file
  val pricesSource = Source.fromFile("C:\\Users\\d.raorane\\Desktop\\HarvestAssignment\\prices.csv")

  // ---------------- FORMAT FUNCTION ----------------
  def format(value: Double): String = f"$value%.2f"


  val pricesMap: Map[(String,LocalDate),Double] = {
    try{
      pricesSource.getLines()
        .drop(1)
        .flatMap{
          line =>
            val parts = line.split(",")
            if(parts.length == 3){
              val fruit = parts(0)
              val date = LocalDate.parse(parts(1))
              val price = parts(2).toDoubleOption.getOrElse(0.0)
              Some((fruit,date) -> price)
            }
            else {
              None
            }
        }.toMap
    }
    finally {
      pricesSource.close()
    }
  }

  // ---------------- LOAD HARVEST ----------------
  val harvestSource = Source.fromFile("C:\\Users\\d.raorane\\Desktop\\HarvestAssignment\\harvest.csv")

  val entries: List[Entry] = {
    try{
      harvestSource.getLines()
        .drop(1)
        .flatMap{
          line =>
            val parts = line.split(",")
            if(parts.length == 4){
              val name = parts(0)
              val date = LocalDate.parse(parts(1))
              val fruit = parts(2)
              val amount = parts(3).toDoubleOption.getOrElse(0.0)

              val price = pricesMap.getOrElse((fruit, date), 0.0)
              Some(Entry(name,date, fruit,amount,amount * price))
            }
            else{
              None
            }
        }.toList
    }
    finally {
      harvestSource.close()
    }
  }

  //Helper functions
  def sumRevenue(list:List[Entry]): Double = {
    list.map(x => x.earning).sum
  }

  def sumAmount(list: List[Entry]): Double = {
    list.map(x => x.amount).sum
  }

  def getMonth(date: LocalDate): String = {
    f"${date.getYear}--${date.getMonthValue}%02d"
  }

  // groupings
  val byMonth = entries.groupBy(x => getMonth(x.date))
  val byFruit = entries.groupBy(_.fruit)
  val byGatherer = entries.groupBy(_.gatherer)

// ------------ BEST GATHERER PER MONTH ---------
  println("\nBest Gatherer Per Month:")
  byMonth.foreach{
    case(month,data) =>
      val best = data.groupBy(_.gatherer)
      .map{ case(g,list) => g -> sumAmount(list) }
      .maxBy(_._2)
      println(s"$month -> ${best._1}(${format(best._2)})")
  }




  //  BEST GATHERER PER FRUIT
  println("\nBest Gatherer Per Fruit:")
  byFruit.foreach{
    case(fruit,data) =>
      val best = data.groupBy(_.gatherer)
        .map { case (g, list) => g -> sumAmount(list) }
        .maxBy(_._2)

      println(s"$fruit -> ${best._1} (${format(best._2)})")
  }

  //  BEST & LEAST PROFITABLE FRUITS
  val fruitRevenue = byFruit.map{
    case(fruit,data) =>
      fruit -> sumRevenue(data)
  }

  val bestFruit = fruitRevenue.maxBy(_._2)
  val leastFruit = fruitRevenue.minBy(_._2)

  println(s"\nBest Fruit Overall: ${bestFruit._1} (${format(bestFruit._2)})")
  println(s"Least Fruit Overall: ${leastFruit._1} (${format(leastFruit._2)})")





  println("\nBest & Least Fruit Per Month:")
  byMonth.foreach {
    case (month, data) =>
    val fruitMap = data.groupBy(_.fruit)
      .map { case (fruit, list) => fruit -> sumRevenue(list) }

    val best = fruitMap.maxBy(_._2)
    val least = fruitMap.minBy(_._2)

    println(s"$month -> Best: ${best._1} (${format(best._2)}), " + s"Least: ${least._1} (${format(least._2)})")
  }




  // TOP GATHERER BY INCOME
  val gathererRevenue = byGatherer.map { case (g, data) =>
    g -> sumRevenue(data)
  }

  val topOverall = gathererRevenue.maxBy(_._2)
  println(s"\nTop Gatherer Overall (Income): ${topOverall._1} (${format(topOverall._2)})")



}

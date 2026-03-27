package lectures.assign

import java.time.LocalDate
import scala.io.Source

object Harvest extends App {

  case class Entry(gatherer: String, date: LocalDate, fruit: String, amount: Double, earning: Double)

  def format(value: Double): String = f"$value%.2f"

  
  // Load Prices
  val pricesSource = Source.fromFile("C:\\Users\\d.raorane\\Desktop\\HarvestAssignment\\prices.csv")

  val pricesMap: Map[(String, LocalDate), Double] =
    try {
      pricesSource.getLines()
        .drop(1)
        .flatMap { line =>
          val parts = line.split(",")
          if (parts.length == 3) {
            val fruit = parts(0)
            val date = LocalDate.parse(parts(1))
            val price = parts(2).toDoubleOption.getOrElse(0.0)
            Some((fruit, date) -> price)
          } else None
        }
        .toMap
    } finally {
      pricesSource.close()
    }

  
  
  // Load Harvest
  val harvestSource = Source.fromFile("C:\\Users\\d.raorane\\Desktop\\HarvestAssignment\\harvest.csv")

  val entries: List[Entry] =
    try {
      harvestSource.getLines()
        .drop(1)
        .flatMap { line =>
          val parts = line.split(",")
          if (parts.length == 4) {
            val name = parts(0)
            val date = LocalDate.parse(parts(1))
            val fruit = parts(2)
            val amount = parts(3).toDoubleOption.getOrElse(0.0)

            val price = pricesMap.getOrElse((fruit, date), 0.0)
            Some(Entry(name, date, fruit, amount, amount * price))
          } else None
        }
        .toList
    } finally {
      harvestSource.close()
    }
    
  

  
  // Helper Functions (Utility)
  def sumRevenue(list: List[Entry]): Double =
    list.map(_.earning).sum

  def sumAmount(list: List[Entry]): Double =
    list.map(_.amount).sum

  def getMonth(date: LocalDate): String =
    f"${date.getYear}-${date.getMonthValue}%02d"



  //  Groupings
  val byMonth = entries.groupBy(e => getMonth(e.date))
  val byFruit = entries.groupBy(_.fruit)
  val byGatherer = entries.groupBy(_.gatherer)



  //  1. BEST GATHERER PER MONTH (BY AMOUNT)
  println("\n1. Best Gatherer (by Quantity) Per Month:")
  byMonth.foreach { case (month, data) =>
    val best = data.groupBy(_.gatherer)
      .map { case (g, list) => g -> sumAmount(list) }
      .maxBy(_._2)

    println(s"$month -> ${best._1} (${format(best._2)})")
  }



  
  
  //  2. BEST GATHERER OVERALL (BY AMOUNT)
  val gatherAmount = byGatherer.map {
    case (g, data) =>  g -> sumAmount(data)
  }
  val bestAmount = gatherAmount.maxBy(_._2)

  println("\n2. Best Gatherer Overall (by Quantity):")
  println(s"${bestAmount._1} (${format(bestAmount._2)})")





  //  3. BEST GATHERER PER FRUIT
  println("\n3. Best Gatherer Per Fruit:")
  byFruit.foreach {
    case (fruit, data) =>
    val best = data.groupBy(_.gatherer)
      .map { case (g, list) => g -> sumAmount(list) }
      .maxBy(_._2)

    println(s"$fruit -> ${best._1} (${format(best._2)})")
  }

  
  
  //  4 & 5. BEST & LEAST EARNING FRUIT OVERALL
  val fruitRevenue = byFruit.map { case (fruit, data) =>
    fruit -> sumRevenue(data)
  }

  val bestFruit = fruitRevenue.maxBy(_._2)
  val leastFruit = fruitRevenue.minBy(_._2)

  println("\n4. Best Earning Fruit Overall:")
  println(s"${bestFruit._1} (${format(bestFruit._2)})")

  println("\n5. Least Earning Fruit Overall:")
  println(s"${leastFruit._1} (${format(leastFruit._2)})")



  //  6 & 7. BEST & LEAST EARNING FRUIT PER MONTH
  println("\n6 & 7. Best and Least Earning Fruit Per Month:")
  byMonth.foreach { case (month, data) =>
    val fruitMap = data.groupBy(_.fruit)
      .map { case (fruit, list) => fruit -> sumRevenue(list) }

    val best = fruitMap.maxBy(_._2)
    val least = fruitMap.minBy(_._2)

    println(s"$month -> Best: ${best._1} (${format(best._2)}), Least: ${least._1} (${format(least._2)})")
  }




  //  8. TOP GATHERER OVERALL (BY INCOME)
  val gathererRevenue = byGatherer.map { case (g, data) =>
    g -> sumRevenue(data)
  }

  val topOverall = gathererRevenue.maxBy(_._2)

  println("\n8. Top Gatherer Overall (by Income):")
  println(s"${topOverall._1} (${format(topOverall._2)})")





  //  9. TOP GATHERER PER MONTH (BY INCOME)
  println("\n9. Top Gatherer Per Month (by Income):")
  byMonth.foreach { case (month, data) =>
    val gMap = data.groupBy(_.gatherer)
      .map { case (g, list) => g -> sumRevenue(list) }

    val best = gMap.maxBy(_._2)
    println(s"$month -> ${best._1} (${format(best._2)})")
  }
}

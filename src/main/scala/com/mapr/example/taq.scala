package com.mapr.example

import org.apache.spark._
import org.apache.spark.sql._
import org.joda.time.{DateTime,Interval}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.util.Date

object TaqParse {

  // case class to lend schema to our data for later
  // conversion to DataFrame.
  case class TradeData(
      tradeTime: Long,
      bar: Long,
      exchange: String,
      symbol: String,
      saleCondition: String,
      tradeVolume: Integer,
      tradePrice: Double,
      tradeStopStockIndicator: String,
      tradeCorrectionIndicator: String,
      tradeSequenceNumber: Long,
      tradeSource: String,
      tradeReportingFacility: String,
      participantTimestamp: String,
      regionalReferenceNumber: String,
      tradeReportingFacilityTimestamp: String,
      lineChange: String)

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TaqParse <inputfile> <outputdir> <secondsPerBar>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("TaqParse")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // The input file we want to process.
    val file = sc.textFile(args(0))

    val outputPath = args(1).toString()
    val barIntervalInSeconds = args(2).toInt

    val tradeDateString = file.first().trim.split("\\s+")(0)

    // Generate our "bars" which are just the joda Intervals spanning the beginning and
    // end of the bar. These go from midnight to midnight, since TAQ data includes after
    // hours activity.
    val fmt = DateTimeFormat.forPattern("'N'MMddyyyy HHmmss")
    val marketOpenTime =  fmt.parseDateTime(s"${tradeDateString} 000000")
    val marketCloseTime =  marketOpenTime.plusDays(1)
    val bars = marketOpenTime.getMillis.to(marketCloseTime.getMillis, barIntervalInSeconds*1000)
      .map(n => new Interval(n, n+(barIntervalInSeconds*1000)))

    val trades = file.filter(line => !isHeaderLine(line)) // filter the header line
      .filter(line => line.contains('@'))                 // filter for regular trades
      .map(x => parseTaqTradeLine(x, tradeDateString, bars)) //  parse the line into a TradeData case class
      .toDF()

    // N.B. this rollup does not include bars where no trades took
    // place for the symbol.
    val tradeRollupByBar = trades.rollup($"symbol", $"bar").agg(Map(
      "tradePrice" -> "avg",
      "tradePrice" -> "max",
      "tradePrice" -> "min",
      "tradeVolume" -> "sum"
    ))

    //val selectedSymbols = trades.filter(t => List("AAPL").contains(t.symbol))
    trades.write.parquet(outputPath)

    sc.stop()
  }

  def isHeaderLine(line: String) = {
    line.contains("Record Count")
  }


  def parseTaqTradeLine(line: String, tradeDate: String, bars: Seq[Interval]) = {
    val lineList = line.toList
    val timeString = lineList.slice(0,9).mkString
    val dateString = s"${tradeDate} ${timeString}"
    val tradeTime = DateTimeFormat.forPattern("'N'MMddyyyy HHmmssSSS").parseDateTime(dateString)
    val exchange = lineList.slice(9,10)
    val symbol = lineList.slice(10,26)
    val saleCondition = lineList.slice(26,30)
    val tradeVolume = lineList.slice(30,39)
    val tradePriceString = lineList.slice(39,50)
    val tradePrice = s"${tradePriceString.slice(0,7).mkString}.${tradePriceString.slice(7,11).mkString}".toDouble
    val tradeStopStockIndicator = lineList.slice(50,51)
    val tradeCorrectionIndicator = lineList.slice(51,53)
    val tradeSequenceNumber = lineList.slice(53,69)
    val tradeSource = lineList.slice(69,70)
    val tradeReportingFacility = lineList.slice(70,71)
    val participantTimestamp = lineList.slice(71,83)
    val regionalReferenceNumber = lineList.slice(83,91)
    val tradeReportingFacilityTimestamp = lineList.slice(91,103)
    val lineChange = lineList.slice(103,105)

    val bar = bars.filter(i => i.contains(tradeTime))

    TradeData(tradeTime.getMillis(),
      bar(0).getStartMillis(),
      exchange.mkString,
      symbol.mkString.trim(),
      saleCondition.mkString.trim(),
      tradeVolume.mkString.toInt,
      tradePrice,
      tradeStopStockIndicator.mkString.trim(),
      tradeCorrectionIndicator.mkString.trim(),
      tradeSequenceNumber.mkString.toLong,
      tradeSource.mkString.trim(),
      tradeReportingFacility.mkString.trim(),
      participantTimestamp.mkString.trim(),
      regionalReferenceNumber.mkString.trim(),
      tradeReportingFacilityTimestamp.mkString,
      lineChange.mkString.trim())
  }

}

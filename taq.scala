package org.apache.spark.examples

import org.apache.spark._
import java.text.SimpleDateFormat
import java.util.Date

object TaqParse {

  case class TradeData(
      tradeTime: Date,
      exchange: String,
      symbol: String,
      saleCondition: String,
      tradeVolume: Integer,
      tradePrice: Double,
      tradeStopStockIndicator: String,
      tradeCorrectionIndicator: String,
      tradeSequenceNumber: Integer,
      tradeSource: String,
      tradeReportingFacility: String,
      participantTimestamp: String,
      regionalReferenceNumber: String,
      tradeReportingFacilityTimestamp: String,
      lineChange: String)

  /** Usage: TaqParse [file] */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TaqParse <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("TaqParse")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(args(0))
    val barIntervalInSeconds = args(1).toInt

    val tradeDateString = file.first().trim.split("\\s+")(0)
    val trades = file.filter(line => !isHeaderLine(line)) // filter the header line
      .filter(line => line.contains('@'))                 // filter for regular trades
      .map(x => parseTaqTradeLine(x, tradeDateString))    //  parse the line into a TradeData case class

    trades.take(10000).map(t => println(t))

    sc.stop()
  }

  def isHeaderLine(line: String) = {
    line.contains("Record Count")
  }

  def parseTaqTradeLine(line: String, tradeDate: String) = {
    val lineList = line.toList
    val timeString = lineList.slice(0,9).mkString
    val dateString = s"${tradeDate} ${timeString}"
    val tradeTime = new SimpleDateFormat("'N'MMddyyyy HHmmssSSS").parse(dateString)

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

    TradeData(tradeTime,
      exchange.mkString,
      symbol.mkString.trim(),
      saleCondition.mkString.trim(),
      tradeVolume.mkString.toInt,
      tradePrice,
      tradeStopStockIndicator.mkString.trim(),
      tradeCorrectionIndicator.mkString.trim(),
      tradeSequenceNumber.mkString.toInt,
      tradeSource.mkString.trim(),
      tradeReportingFacility.mkString.trim(),
      participantTimestamp.mkString.trim(),
      regionalReferenceNumber.mkString.trim(),
      tradeReportingFacilityTimestamp.mkString,
      lineChange.mkString.trim())
  }

}

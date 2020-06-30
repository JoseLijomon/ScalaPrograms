package com.info.basics.assignment

/**
 * Created by Lijomon on 30th June 2020.
 */
import org.apache.spark.{SparkConf, SparkContext}

object AirlinesRDD {

  val airlinesPath="airlines.csv"
  val airportsPath="airports.csv"
  val flightsPath= "flights.csv"

  def main(args: Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("Spark Study").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val airlines=sc.textFile(airlinesPath)
    airlines.filter(SimpleApp.noHeader).take(10).foreach(println)
    val airlineNoHeader = airlines.filter(SimpleApp.noHeader)

    println("Printing data from flights file ")
    val flights=sc.textFile(flightsPath)
    flights.take(5).foreach(println)

    //Parse the flight data
    println("Printing parsed flight data ")
    val parsedFlights = flights.map(SimpleApp.parse)
    parsedFlights.take(10).foreach(println)

    println("Create a pair RDD with origin as a key ")
    val airportDelays = parsedFlights.map( x => (x.origin,x.dep_delay))

    println("Print few keys and values")
    airportDelays.keys.take(10).foreach(print)
    airportDelays.values.take(10).foreach(print)

    val airportTotalDelayRDD = airportDelays.reduceByKey((x,y) => x+y)
    println("Create a pair RDD to calculate the count of flightd by airports")

    val airportsDelayedFligtsRDD = airportDelays.mapValues( x => 1).reduceByKey( (x,y) => x+y)
    val airportsDelaysAndCounts = airportTotalDelayRDD.join(airportsDelayedFligtsRDD)

    println("Calculate the average delays ")
    val airportAvgDelays = airportsDelaysAndCounts.mapValues( x => x._1/x._2.toDouble)

    airportAvgDelays.take(10).foreach( x =>{ println(f"${x._1}%s has average delay of ${x._2}%f") })
    println("Top 10 Average delays by airports")
    airportAvgDelays.sortBy(-_._2).take(10).foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})

    val avgFlightSumAndCount = airportAvgDelays.combineByKey( value => (value,1), (record :(Double,Int), value) => (record._1+value,record._2+1), (record1 :(Double,Int), record2 :(Double,Int)) => (record1._1 + record2._1 , record1._2+record2._2))
    println("Airport with average flight delays")

    val airportAvgDelaysCombineBy = avgFlightSumAndCount.mapValues(x=> x._1/x._2)
    airportAvgDelaysCombineBy.sortBy(-_._2).take(10).foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})

    val airportsPairRDD = sc.textFile(airportsPath).filter(SimpleApp.noHeader).map(parseLookup)
    airportsPairRDD.take(10).foreach( x => { println(f" ${x._1}%s and description is ${x._2}%s") })

    val airportLookupRDD=airportsPairRDD.collectAsMap
    println("Description of CLI is ",  airportLookupRDD("CLI"))

    val airportBCRDD=sc.broadcast(airportLookupRDD)
    airportAvgDelays.map(x => (airportBCRDD.value(x._1),x._2)).sortBy(-_._2).take(10).foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})
  }

  def parseLookup(row: String) : (String,String) = {
    val x = row.replace("\"","").split(",")
    (x(0),x(1))
  }
}
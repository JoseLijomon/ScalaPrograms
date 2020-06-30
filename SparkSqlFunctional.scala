package com.info.basics.assignment

/**
 * Created by Lijomon on 30th June 2020.
 */
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlFunctional {

  val airlinesPath="airlines.csv"
  val airportsPath="airports.csv"
  val flightsPath= "flights.csv"

  def main(args: Array[String]): Unit = {
    val spSession = SparkSession.builder().master("local[*]").appName(name = "Spark-Sql").getOrCreate();
    loadCSVData(spSession)
    findAirLineByName(spSession)
    findLongJourneyFlight(spSession)
    findAvgDelay(spSession)
    countFlightsBySource(spSession)
    countFlightsBySourceAndDest(spSession)
  }

  var airlines: DataFrame = _;
  var flights: DataFrame = _;
  var airports: DataFrame = _;

  // Loading data frames
  def loadCSVData(sparkSession: SparkSession): Unit = {
    this.airlines = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load(airlinesPath)
    this.flights = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load(flightsPath)
    this.airports = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load(airportsPath)
    airlines.createOrReplaceTempView("airlines")
    flights.createOrReplaceTempView("flights")
    airports.createOrReplaceTempView("airports")
  }

  /**
   * Task 1 :- Find the airlines name and total number of flights run by these airlines
   */
  def getTotalFlightByName(sparkSession: SparkSession) = {
    sparkSession.sql("select airline.code, airline.description, count(flght.flight_number) as num_of_flights from airlines airline " +
          "INNER JOIN flights flght ON airline.code == flght.airlines GROUP BY airline.code, airline.description ").show()
  }

  /**
   * Task 2 :- Airline info (Flight Number, Airlines Number, Airlines Name), Description (running flights to LAX (Los Angeles, CA: Los Angeles International)
   */
  def findAirLineByName(sparkSession: SparkSession) = {
    sparkSession.sql("select  flght.flight_number, flght.airlines as airlines_num, ap.description as destination , airline.description as airlines_name from airlines airline inner join flights flght on airline.code = flght.airlines inner join airports ap on flght.origin = ap.code where ap.description='Los Angeles, CA: Los Angeles International' ").show()
  }

  /**
   * Task 3 :- distance origin and destination (Print flight code, name, origin and destination which are traveled largest distance)
   */
  def findLongJourneyFlight(sparkSession: SparkSession): Unit = {
    sparkSession.sql("select flght.flight_number, flght.airlines as airlines_num," +
      " flght.origin, flght.destination , flght.distance, airline.description as airlines_name from airlines airline " +
      " inner join flights flght on airline.code = flght.airlines inner join airports ap on flght.origin = ap.code order by flght.distance desc LIMIT 1").show()
  }

  /**
   * Task 4 :- Airlines info (Airlines number and airline name - average delay for all flights they are running)
   */
  def findAvgDelay(sparkSession: SparkSession): Unit = {
    sparkSession.sql("select airline.code as airline_num, airline.description as airline_name, avg(flght.departure_delay) as delay from airlines airline INNER JOIN flights flght ON airline.code == flght.airlines GROUP BY airline.code, airline.description ").show()
  }

  /**
   * Task 5 :- Total count of flights running from each origin (No Records for Hyderabad, Mumbai and Chennai)
   */
  def countFlightsBySource(sparkSession: SparkSession) = {
    sparkSession.sql("select origin, count(*) as num_of_flights from flights group by origin order by num_of_flights desc").show(5)
  }

  /**
   * Task 6 :- Total count of flights running from each origin to destination (No Records for Hyderabad, Mumbai and Chennai)
   */
  def countFlightsBySourceAndDest(sparkSession: SparkSession): Unit = {
    sparkSession.sql("select origin, destination, count(*) as num_of_flights from flights group by origin, destination order by num_of_flights desc").show(5)
  }
}
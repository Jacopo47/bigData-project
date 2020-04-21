package spark

import model.{Airline, Utils, Flight}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkJob {
  private val getAirlines = (sc: SparkContext) => {
    sc.textFile(Utils.FULL_PATH_FILE_AIRLINES)
      .map(Airline.extract)
      .filter(_.isDefined)
      .map(_.get)
      .map(e => (e.iataCode, e.airline))
  }

  private val getFlights: (SparkContext, String) => RDD[Flight] = (sc: SparkContext, input: String) => {
    sc.textFile(input)
      .map(Flight.extract)
      .filter(_.isDefined)
      .map(_.get)
  }

  def classicSparkJob(csvFilePath: String): Unit = {
    val sc: SparkContext = SparkSession.builder.appName("BigDataProject - Spark2").getOrCreate().sparkContext

    val rddFlights = getFlights(sc, csvFilePath)
      .filter(!_.cancelled)
      .filter(_.arrivalDelay > 0)
      .map(flight => (flight.iataCode, flight))

    val initialValue = (0.0, 0.0)
    val combiningFunction: ((Double, Double), Flight) => (Double, Double) = (accumulator: (Double, Double), flight: Flight) => {
      (accumulator._1 + flight.arrivalDelay, accumulator._2 + flight.distance)
    }
    val mergingFunction = (accumulator1: (Double, Double), accumulator2: (Double, Double)) => {
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
    }


    val flightDelayKpiByAirline = rddFlights
      .aggregateByKey(initialValue)(combiningFunction, mergingFunction)
      .map({ case (k, v) => (k, v._1 / v._2) })

    val rddAirlines = getAirlines(sc)

    val result = rddAirlines
      .join(flightDelayKpiByAirline)
        .map({ case (_, v) => v })
        .sortBy(_._2)

    result.collect foreach println
  }

  def sparkSql(csvFilePath: String): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("BigDataProject - Spark2 SQL").getOrCreate()
    sparkSession.sql("use default")
    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val dfFlights = getFlights(sc, csvFilePath).toDF()
      .filter("cancelled == false AND arrivalDelay > 0")
      .groupBy("iataCode")
      .agg(sum("distance").as("distance"), sum("arrivalDelay").as("arrivalDelay"))
      .withColumn("KPI", col("arrivalDelay").divide(col("distance")))

    dfFlights.createOrReplaceTempView("flights")

    val dfAirlines = getAirlines(sc).map(a => Airline(a._1, a._2)).toDF()
    dfAirlines.createOrReplaceTempView("airlines")

    val dfJoin = sparkSession.sql("SELECT a.airline, f.KPI FROM flights AS f, airlines AS a WHERE a.iataCode = f.iataCode ORDER BY f.KPI")

    dfJoin.collect foreach println
  }
}
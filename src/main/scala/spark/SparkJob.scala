package spark

import model.{Airline, Flight, SparkElements, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import spark.SparkJob.{getAirlines, getFlights}

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


  def apply(contextName: String, logLevel: Option[String]): SparkJob = {
    val sparkSession = SparkSession.builder.appName(contextName).getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel(logLevel.getOrElse("INFO"))

    new SparkJob(SparkElements(sparkSession, sparkContext))
  }
}

class SparkJob(sparkElements: SparkElements) {
  private val aggregateFlights: RDD[(String, Flight)] => RDD[(String, Double)] = (flights: RDD[(String, Flight)]) => {
    val initialValue = (0.0, 0.0)
    val combiningFunction: ((Double, Double), Flight) => (Double, Double) = (accumulator: (Double, Double), flight: Flight) => {
      (accumulator._1 + flight.arrivalDelay, accumulator._2 + flight.distance)
    }
    val mergingFunction = (accumulator1: (Double, Double), accumulator2: (Double, Double)) => {
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
    }


    flights
      .aggregateByKey(initialValue)(combiningFunction, mergingFunction)
      .map({ case (k, v) => (k, v._1 / v._2) })
  }

  def classicSparkJob(csvFilePath: String): Unit = {
    val sc: SparkContext = sparkElements.sparkContext

    val rddFlights: RDD[(String, Flight)] = getFlights(sc, csvFilePath)
      .filter(!_.cancelled)
      .filter(_.arrivalDelay > 0)
      .map(flight => (flight.iataCode, flight))


    val flightDelayKpiByAirline = aggregateFlights(rddFlights)
    val rddAirlines = getAirlines(sc)

    val result = rddAirlines
      .join(flightDelayKpiByAirline)
      .map({ case (_, v) => v })
      .sortBy(_._2)

    result.collect foreach println
  }

  def sparkSql(csvFilePath: String): Unit = {
    val sparkSession: SparkSession = sparkElements.sparkSession
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

  def sparkStreaming(monitoredDirectory: String): Unit = {
    val sparkSession: SparkSession = sparkElements.sparkSession
    val sc: SparkContext = sparkSession.sparkContext

    def updateFunction(newValues: Seq[Double], oldValue: Option[(String, Double)]): Option[(String, Double)] = {
      oldValue match {
        case Some(old) => Some(old._1, old._2 + newValues.sum)
        case None => None
      }
    }

    val newSsc = new StreamingContext(sc, Seconds(3))
    val flights: DStream[(String, Flight)] = newSsc
      .textFileStream(monitoredDirectory)
      .map(Flight.extract)
      .filter(_.isDefined)
      .map(_.get)
      .filter(!_.cancelled)
      .filter(_.arrivalDelay > 0)
      .map(flight => (flight.iataCode, flight))

    val result: DStream[(String, Double)] = flights.transform(rddFlights => {
      aggregateFlights(rddFlights)
    })

    //val rank = result.map(e => (e._1, e._2)).updateStateByKey(updateFunction)

    result.print()

    newSsc.start()
    newSsc.awaitTermination()
  }
}
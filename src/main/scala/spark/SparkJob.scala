package spark

import model._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

  private val basicAggregateFlights: RDD[(String, Flight)] => RDD[(String, (Double, Double))] = (flights: RDD[(String, Flight)]) => {
    val initialValue = (0.0, 0.0)
    val combiningFunction: ((Double, Double), Flight) => (Double, Double) = (accumulator: (Double, Double), flight: Flight) => {
      (accumulator._1 + flight.arrivalDelay, accumulator._2 + flight.distance)
    }
    val mergingFunction = (accumulator1: (Double, Double), accumulator2: (Double, Double)) => {
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
    }


    flights
      .aggregateByKey(initialValue)(combiningFunction, mergingFunction)
      .map({ case (k, v) => (k, (v._1, v._2)) })
  }

  private val aggregateFlights: RDD[(String, Flight)] => RDD[(String, Double)] = (flights: RDD[(String, Flight)]) => {
    basicAggregateFlights(flights).map({ case (k, v) => (k, v._1 / v._2) })
  }

  private val aggregateFlightsInStatistics: RDD[(String, Flight)] => RDD[(String, AirlineStatistics)] = (flights: RDD[(String, Flight)]) => {
    basicAggregateFlights(flights)
      .map({ case (k, v) => (k, AirlineStatistics(v._1, v._2, v._1 / v._2)) })
  }

  def getSparkElements(contextName: String, logLevel: Option[String]): SparkElements = {
    val sparkSession = SparkSession.builder.appName(contextName).getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel(logLevel.getOrElse("INFO"))

    SparkElements(sparkSession, sparkContext)
  }


  def classicSparkJob(csvFilePath: String, sparkElements: SparkElements): Unit = {
    val sc: SparkContext = sparkElements.sparkContext

    val rddFlights: RDD[(String, Flight)] = getFlights(sc, csvFilePath)
      .filter(!_.cancelled)
      .filter(_.arrivalDelay > 0)
      .map(flight => (flight.iataCode, flight))
      .partitionBy(new HashPartitioner(8))


    val flightDelayKpiByAirline = aggregateFlights(rddFlights)
    val rddAirlines = getAirlines(sc)

    val result = rddAirlines
      .join(flightDelayKpiByAirline)
      .map({ case (_, v) => v })
      .sortBy(_._2)

    result.collect foreach println
  }

  def sparkSql(csvFilePath: String, sparkElements: SparkElements): Unit = {
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

  def sparkStreaming2(monitoredDirectory: String, sparkElements: SparkElements): Unit = {
    val sparkSession: SparkSession = sparkElements.sparkSession
    val sc: SparkContext = sparkSession.sparkContext
    val checkpointDirectory = monitoredDirectory + "/../checkpoint"


    def funcToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(3))
      Log.error("EHI - 1")
      val flights: DStream[(String, Flight)] = newSsc
        .textFileStream(monitoredDirectory)
        .map(Flight.extract)
        .filter(_.isDefined)
        .map(_.get)
        .filter(!_.cancelled)
        .filter(_.arrivalDelay > 0)
        .map(flight => (flight.iataCode, flight))

      try {
        val result: DStream[(String, AirlineStatistics)] = flights.transform(rddFlights => {
          aggregateFlightsInStatistics(rddFlights)
        })

        val rank = result.map(e => (e._1, e._2)).updateStateByKey((newValues: Seq[AirlineStatistics], oldValue: Option[AirlineStatistics]) => {
          val valuesSum = newValues.reduceLeftOption((a, b) => AirlineStatistics(a.totalArrivalDelay + b.totalArrivalDelay, a.totalDistance + b.totalDistance, 0)).getOrElse(AirlineStatistics(0, 0, 0))
          oldValue match {
            case Some(value) =>
              val app = AirlineStatistics(valuesSum.totalArrivalDelay + value.totalArrivalDelay, valuesSum.totalDistance + value.totalDistance, 0)
              Some(AirlineStatistics(app.totalArrivalDelay, app.totalDistance, app.totalArrivalDelay / app.totalDistance))
            case None => Some(AirlineStatistics(valuesSum.totalArrivalDelay, valuesSum.totalDistance, valuesSum.totalArrivalDelay / valuesSum.totalDistance))
          }
        })
        rank.print()
      } catch {
        case ex: Exception => Log.debug(ex.getMessage)
      }

      newSsc.checkpoint(checkpointDirectory)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, funcToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }
}
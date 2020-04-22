package spark

import model.{Airline, Flight, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

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

  def sparkStreaming(csvFilePath: String): Unit = {
    def exercise4(sc: SparkContext, host: String, port: Int, path: String): Unit = {
      def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
        Some(oldValue.getOrElse(0) + newValues.sum)
      }

      def functionToCreateContext(): StreamingContext = {
        val newSsc = new StreamingContext(sc, Seconds(3))
        val lines = newSsc.socketTextStream(host,port,StorageLevel.MEMORY_AND_DISK_SER)
        val words = lines.flatMap(_.split(" "))
        val cumulativeWordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunction)
        cumulativeWordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
        newSsc.checkpoint(path)
        newSsc
      }

      val ssc = StreamingContext.getOrCreate(path, functionToCreateContext _)
      ssc.start()
      ssc.awaitTermination()
    }
  }
}
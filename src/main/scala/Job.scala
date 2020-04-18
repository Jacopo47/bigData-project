import model.{Airline, Flight}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Job extends App {

  override def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    classicSparkQuery(sc)
  }

  def classicSparkQuery(sc: SparkContext): Unit = {
    val rddFlights = sc.textFile("hdfs:/user/jriciputi/bigdata/dataset/flight.csv")
      .map(Flight.extract)
      .filter(_.isDefined)
      .map(_.get)
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
      .sortBy(_._2, false)

    val rddAirlines = sc
      .textFile("hdfs:/user/jriciputi/bigdata/dataset/flight.csv")
      .map(Airline.extract)
      .filter(_.isDefined)
      .map(_.get)
      .groupBy(_.iataCode)

    val result = rddAirlines.join(flightDelayKpiByAirline)

    result.collect foreach println
  }

  /**
   * Creates the SparkContent; comment/uncomment code depending on Spark's version!
   *
   * @return
   */
  def getSparkContext(): SparkContext = {
    // Spark 2
    val spark = SparkSession.builder.appName("Exercise 302 - Spark2").getOrCreate()
    spark.sparkContext
  }

}
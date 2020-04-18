import model.{Log, Utils}
import org.joda.time.{DateTime, Period}
import spark.SparkJob

object Main extends App {
  val SPARK_JOB_TYPE = "0"
  val SPARK_SQL_JOB_TYPE = "1"
  val MAP_REDUCE_JOB_TYPE = "2"

  val argsAsList = args.toSeq

  val jobType = argsAsList.headOption match {
    case Some(jobT) => jobT
    case None => "0"
  }

  val csvFilePath = Utils.DATASET_PATH + (argsAsList.lift(1) match {
    case Some(fileName) => fileName
    case None => Utils.DEFAULT_FILE_FLIGHTS
  })

  val startTime = new DateTime()
  Log.info("Starting job at: " + startTime.toString("HH:mm"))
  try {
    jobType match {
      case SPARK_JOB_TYPE => SparkJob.classicSparkJob(csvFilePath)
      case SPARK_SQL_JOB_TYPE => SparkJob.sparkSql(csvFilePath)
      case MAP_REDUCE_JOB_TYPE => new MapReduceJob().start(csvFilePath, Utils.DATASET_PATH)

      case _ => SparkJob.classicSparkJob(csvFilePath)
    }
  } catch {
    case ex: Exception => Log.error(s"Error during job running. Details: ${ex.getMessage}\n${ex.printStackTrace()}")
  }

  val endTime = new DateTime()
  Log.info(s"Job is end! Start time: ${startTime.toString("HH:mm:ss")} - End time: " + endTime.toString("HH:mm:ss"))
  val difference = new Period(endTime.getMillis - startTime.getMillis)
  Log.info(s"Job time: ${difference.getMinutes}m : ${difference.getSeconds}s")
}

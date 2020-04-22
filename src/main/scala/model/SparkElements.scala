package model

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class SparkElements(sparkSession: SparkSession, sparkContext: SparkContext)
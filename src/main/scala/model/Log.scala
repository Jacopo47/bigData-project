package model

import org.apache.log4j.lf5.LogLevel
import org.apache.log4j.{Level, LogManager}


object Log {
  private val logger = LogManager.getLogger(Log.getClass)

  def setLevel(logLevel: Option[String]): Unit = {
    val level: String = logLevel.getOrElse("INFO")

    val selectedLevel: Level = level match {
      case "ALL" => Level.ALL
      case "DEBUG" => Level.DEBUG
      case "INFO" => Level.INFO
      case "WARN" => Level.WARN
      case "ERROR" => Level.ERROR
      case "OFF" => Level.OFF
      case _ => Level.INFO
    }

    logger.setLevel(selectedLevel)
  }

  def debug(msg: String): Unit = logger.debug(msg)

  def info(msg: String): Unit = logger.info(msg)

  def warn(msg: String): Unit = logger.warn(msg)

  def error(msg: String): Unit = {
    logger.error(msg)
  }
}
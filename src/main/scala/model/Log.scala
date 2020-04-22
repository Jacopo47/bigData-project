package model

import org.apache.log4j.{Level, LogManager, Logger}


object Log {
  private val logger = LogManager.getLogger(Log.getClass)

  def setRootLevel(logLevel: Option[String]): Unit = {
    setLevel(logLevel, Logger.getRootLogger)
  }

  def setLevel(logLevel: Option[String]): Unit = {
    setLevel(logLevel, logger)
  }

  private def setLevel(logLevel: Option[String], logger: Logger): Unit = {
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
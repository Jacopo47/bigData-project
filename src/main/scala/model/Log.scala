package model

import org.apache.log4j.{Level, LogManager}


object Log {
  private val logger = LogManager.getLogger(Log.getClass)
  logger.setLevel(Level.DEBUG)

  def debug(msg: String): Unit = logger.debug(msg)

  def info(msg: String): Unit = logger.info(msg)

  def warn(msg: String): Unit = logger.warn(msg)

  def error(msg: String): Unit = {
    logger.error(msg)
  }
}
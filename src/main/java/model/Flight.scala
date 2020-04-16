package model

object Flight {
  def extract(row: String): Option[Flight] = {
    def getInt(str: String): Int = {
      if (str.isEmpty)
        0
      else
        str.toInt
    }

    def getBoolean(str: String): Boolean = {
      if (str.isEmpty || str.equals("0")) {
        false
      } else {
        true
      }
    }

    try {

      val columns = row.split(",").map(_.replaceAll("\"", ""))

      val year = getInt(columns(0))
      val airline = columns(4)
      val distance = getInt(columns(17))
      val arrivalDelay = getInt(columns(22))
      val cancelled = getBoolean(columns(24))

      Some(Flight(year, airline, distance, arrivalDelay, cancelled))
    } catch {
      case _: Throwable => None;
    }
  }
}

case class Flight(year: Int, iataCode: String, distance: Int, arrivalDelay: Int, cancelled: Boolean)
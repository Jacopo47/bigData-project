package model

object Airline {
  def extract(row: String): Option[Airline] = {
    try {

      val columns = row.split(",").map(_.replaceAll("\"", ""))

      val iataCode = columns(0)
      val airline = columns(1)

      Some(Airline(iataCode, airline))
    } catch {
      case _: Throwable => None;
    }
  }
}

case class Airline(iataCode: String, airline: String)
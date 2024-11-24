package nl.dhoang.streaming.spark

import org.json4s.DefaultFormats

import java.text.SimpleDateFormat
import java.util.Date

object SimpleEvent {

  private val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def convertStringToDate(dateString: String): Date = format.parse(dateString)

  def fromJson(byteArray: Array[Byte]): SimpleEvent = {
    implicit val formats = DefaultFormats
    val newString = new String(byteArray, "UTF-8")
    val parsed = parse(newString)
    parsed.extract[SimpleEvent]
  }
}

case class SimpleEvent(id: String, timestamp: String, `type`: String) {

  // Convert timestamp into Time Bucket using Bucketing Strategy
  val bucket = BucketingStrategy.bucket(SimpleEvent.convertStringToDate(timestamp))

}

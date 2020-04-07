import scala.io.Source
import org.apache.kafka.clients.producer._
import java.util.Properties
//import play.api.libs.functional.syntax._
import java.util.Timer
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.collection.JavaConverters._

import java.io._

//members in group : Francis Tran, Sebastien Chen, Jérémy Trullier, Zhan Chen
object Consumer extends App {

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "myconsumergroup")

  val consumer = new KafkaConsumer[String, String](props)

  val topic ="dronesmessage"
  consumer.subscribe(util.Collections.singletonList(topic))



  def receiveStream(period: Int): Unit =  {

    val receiv = new java.util.TimerTask {
      def run() = {
        println("receiving data now...")
        val records=consumer.poll(3)
          for (record<-records.asScala){
            println(record)
          }
      }
    }

    val interval = new java.util.Timer()
    interval.schedule(receiv, period, period)
  }

  receiveStream(5000)

}





import scala.io.Source
import org.apache.kafka.clients.producer._
import java.util.Properties
import java.util.Timer

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session}

//members in group : Francis Tran, Sebastien Chen, Jérémy Trullier, Zhan Chen

object Producer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val topic = "dronesmessage"




  //********************configurations for email send START***********************
  val host = "smtp.gmail.com"
  val port = "587"

  val address = "zhan.chen@efrei.net"
  val username = "dronemessage.efrei@gmail.com"
  val password = "drone@efrei.123"

  def sendEmail(mailSubject: String, mailContent: String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(address));
    message.setSubject(mailSubject)
    message.setContent(mailContent, "text/html")


    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)}
    //********************configurations for email send END***********************




  def DroneSendStream(period: Int): Unit =  {

    val fileName = "/home/chen/Documents/dronemessages.csv"

    val send = new java.util.TimerTask {
      def run() = {
        println("info sent by drones!")


        Source.fromFile(fileName).getLines().drop(1).foreach{l =>
          val key = l.split(",") {0}

          if (l.size > 44 )
          {
            println("need intervention, an alert email has been send")
            sendEmail("alterte","one drone needs your intervention")
          }


          val record = new ProducerRecord(topic, key, l)
          producer.send(record)
        }

        println("End")
      }
    }
    val interval = new java.util.Timer()
    interval.schedule(send, period, period)
  }

  DroneSendStream(5000)

}



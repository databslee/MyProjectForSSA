import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object MyProject {
  case class driver(id:Long,date_created_s:String,name:String)
  case class passenger(id:Long,date_created_s:String,name:String)
  case class booking(id:Long,date_created_s:String,id_driver:Long, id_passenger:Long, rating:Int, start_date_s:String, end_date_s:String,  tour_value:Long)

  def main(args: Array[String]): Unit = {

    // Read setup.json file which includes some external data like id, pwd and url.
    var PostgreID = ""
    var PostgrePWD = ""
    var PostgreURL = ""
    var KafkaServer = ""
    var KafkaProducerTopic1 = ""
    var KafkaProducerTopic2 = ""

    try {
      val line = Source.fromFile("setup.json").getLines().mkString
        import net.liftweb.json._
        implicit val formats = DefaultFormats
        case class Setup(PostgreID:String, PostgrePWD:String, PostgreURL:String, KafkaServer:String, KafkaProducerTopic1:String, KafkaProducerTopic2:String)
        PostgreID = parse(line).extract[Setup].PostgreID
        PostgrePWD = parse(line).extract[Setup].PostgrePWD
        PostgreURL = parse(line).extract[Setup].PostgreURL
        KafkaServer = parse(line).extract[Setup].KafkaServer
        KafkaProducerTopic1 = parse(line).extract[Setup].KafkaProducerTopic1
        KafkaProducerTopic2 = parse(line).extract[Setup].KafkaProducerTopic2
    } catch {
        case ex: Exception => println(ex)
        println("ERROR 01 : Check Setup.JSON file.")
        System.exit(1)
    }

    val conf = new SparkConf().setAppName("MyProject-TASK123").setMaster("local[2]")
    var sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Load the condition info for TASK-3
    var conditionKey = ""
    var conditionValue = ""
    try {
      val properties = new Properties()
      properties.put("user", PostgreID)
      properties.put("password", PostgrePWD)
      val conditionTable = sqlContext.read.jdbc(PostgreURL, "conditions", properties)
      def conditionCheck(x: String): String = x match {
        case "over" => ">="
        case "under" => "<="
        case "same" => "="
      }
      conditionKey = conditionCheck(conditionTable.select("con").first.getString(0))
      conditionValue = conditionTable.select("value").first.getInt(0).toString()
    }catch{
      case ex: Exception => println(ex)
      println("ERROR 02 : Check postgresql.")
      System.exit(1)
    }

    // Task running menu.
    println("Please select TASK (1~3)")
    val menu=scala.io.StdIn.readLine()
    if(menu == "1")
      {
        println("Run TASK 1.")
        TASK01(sc, PostgreID, PostgrePWD, PostgreURL)
        println("Complete TASK 1.")
      }else if(menu == "2"){
        println("Run TASK 2.")
        var no = 10001
        TASK02(sc,KafkaServer, KafkaProducerTopic1, no)
        println("Complete TASK 2.")
      }else if(menu == "3"){
        println("Run TASK 3.")
        TASK03(sc,KafkaServer, KafkaProducerTopic1, KafkaProducerTopic2, conditionKey, conditionValue)
        println("Complete TASK 3.")
      }else{
      println("Exit the program.")
    }
  }

  def TASK01(sc: SparkContext, PostgreID:String, PostgrePWD:String, PostgreURL:String): Unit =
  {
    try {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
      val driverFile = sc.textFile("driver.csv").map(x => x.split(",")).map(x => driver(x(0).toLong, x(1), x(2))).toDF()
        .withColumn("date_created", to_timestamp($"date_created_s", "yyyy-MM-dd HH:mm")).select($"id", $"date_created", $"name")
      val passengerFile = sc.textFile("passenger.csv").map(x => x.split(",")).map(x => passenger(x(0).toLong, x(1), x(2))).toDF()
        .withColumn("date_created", to_timestamp($"date_created_s", "yyyy-MM-dd HH:mm")).select($"id", $"date_created", $"name")
      val bookingFile = sc.textFile("booking.csv").map(x => x.split(",")).map(x => booking(x(0).toLong, x(1), x(2).toLong, x(3).toLong, x(4).toInt, x(5), x(6), x(7).toLong)).toDF()
        .withColumn("date_created", to_timestamp($"date_created_s", "yyyy-MM-dd HH:mm"))
        .withColumn("start_date", to_timestamp($"start_date_s", "yyyy-MM-dd HH:mm"))
        .withColumn("end_date", to_timestamp($"end_date_s", "yyyy-MM-dd HH:mm"))
        .select($"id", $"date_created", $"id_driver", $"id_passenger", $"rating", $"start_date", $"end_date", $"tour_value")

      val properties = new Properties()
      properties.put("user", PostgreID)
      properties.put("password", PostgrePWD)
      driverFile.write.mode(SaveMode.Append).jdbc(PostgreURL, "driver", properties)
      passengerFile.write.mode(SaveMode.Append).jdbc(PostgreURL, "passenger", properties)
      bookingFile.write.mode(SaveMode.Append).jdbc(PostgreURL, "booking", properties)
    }catch{
      case ex: Exception => println(ex)
      println("ERROR 03 : Check postgresql in TASK01.")
      System.exit(1)
    }
  }

  def TakeDateTime(s: String): String = {
    s.substring(0,10) + " " + s.toString().substring(11,16)
  }

  def TASK02(sc: SparkContext, KafkaServer:String, KafkaProducerTopic1:String, no:Int): Unit =
  {

    try {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val ssc = new StreamingContext(sc, Seconds(10))

      val dataQueueu = mutable.Queue(GenerateData(no, sc))
      val dataQueueuStream = ssc.queueStream(dataQueueu,false)

      dataQueueuStream.foreachRDD(rdd => {
        val TOPIC=KafkaProducerTopic1
        val props = new Properties()
        props.put("bootstrap.servers", KafkaServer)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        val data = rdd.collect().mkString(",").toString
        val record = new ProducerRecord(TOPIC, "key", data.substring(1, data.length - 1))
        println(data.substring(1, data.length - 1))
        producer.send(record)
        producer.close()
        ssc.stop()
      })

      ssc.start()
      ssc.awaitTermination()

    }catch{
      case ex: Exception => println(ex)
      println("ERROR 04 : Check kafka or spark streaming in TASK02.")
      System.exit(1)
    }
  }

  // Generate data for TASK02
  def GenerateData(no:Int, sc: SparkContext): RDD[(Int, String, Int, Int, Int, String, String, Int)] = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val startTimeValue = 1420070400
    val endTimeValue = 1483228799
    val r = scala.util.Random
    val id_driver = r.nextInt(100)+1
    val id_passenger = r.nextInt(100)+1
    val rating = r.nextInt(5)+1
    val value = r.nextInt(900)+100
    val date_created_long = startTimeValue + r.nextInt(endTimeValue - startTimeValue) + 1
    val start_date_long = date_created_long + r.nextInt(1500) + 300 + 1
    val end_date_long = start_date_long + r.nextInt(1500) + 300 + 1
    val date_created = Instant.ofEpochMilli(date_created_long*1000L)
    val start_date = Instant.ofEpochMilli(start_date_long*1000L)
    val end_date = Instant.ofEpochMilli(end_date_long*1000L)
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val values = List((no, TakeDateTime(date_created.toString()) , id_driver, id_passenger, rating, TakeDateTime(start_date.toString()), TakeDateTime(end_date.toString()), value))
    val dataFrame = values.toDF("id", "date_created","id_driver","id_passenger", "rating", "start_date", "end_date", "tour_value")
    sqlContext.sparkContext.parallelize(values)
  }

  def TASK03(sc: SparkContext, KafkaServer:String, KafkaProducerTopic1:String, KafkaProducerTopic2:String, conditionKey:String, conditionValue:String): Unit =
  {
    try{
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> KafkaServer,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark-task-group",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val ssc = new StreamingContext(sc, Seconds(10))

      val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array(KafkaProducerTopic1), kafkaParams))
      val processedStream = inputStream
        .flatMap(record => record.value.split("\n"))

      processedStream.foreachRDD(s=>{
        println(s)
        s.toDF().createTempView("p")
        val sqlDF = sqlContext.sql(f"""
          select
          split(value, '[,]')[0] _c0,
          split(value, '[,]')[1] _c1,
          split(value, '[,]')[2] _c2,
          split(value, '[,]')[3] _c3,
          split(value, '[,]')[4] _c4,
          split(value, '[,]')[5] _c5,
          split(value, '[,]')[6] _c6,
          split(value, '[,]')[7] _c7
          from p
          where
          unix_timestamp(CONCAT(split(value, '[,]')[6], ':00'))-
          unix_timestamp(CONCAT(split(value, '[,]')[5], ':00'))
          $conditionKey%s  $conditionValue%s
        """)
        sqlContext.dropTempTable("p")
        val sqlRDD = sqlDF.rdd
        sqlRDD.foreach(println)

        sqlRDD.foreach(e => {
          val TOPIC = KafkaProducerTopic2
          val props = new Properties()
          props.put("bootstrap.servers", KafkaServer)
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)
          val record = new ProducerRecord(TOPIC, "key", e.mkString(","))
          producer.send(record)
          producer.close()
        })
      })
      ssc.start()
      ssc.awaitTermination()
    }catch{
      case ex: Exception => println(ex)
      println("ERROR 05 : Check kafka or spark streaming in TASK03.")
      System.exit(1)
    }
  }
}

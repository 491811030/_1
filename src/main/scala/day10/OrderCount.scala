package day10

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.yarn.lib.ZKClient
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object OrderCount {
  def main(args: Array[String]): Unit = {

    val group = "g1"
    val conf: SparkConf = new SparkConf().setAppName("OrderCoount").setMaster("local[4]")

    val ssc = new StreamingContext(conf, Duration(5000))

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = IPUtils.broadcastIpRules(ssc, "/Users/zx/Desktop/temp/spark-24/spark-4/ip/ip.txt")


    val topic = "orders"

    val brokerList = "node-4:9092,node-5:9092,node-6:9092"

    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"

    val topics: Set[String] = Set(topic)

    val topicDirs = new ZKGroupTopicDirs(group, topic)

    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      //"key.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[StringDeserializer],
      //"deserializer.encoding" -> "GB2312", //配置读取Kafka中数据的编码
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    val zkClient = new ZkClient(zkQuorum)

    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0) {

      for (i <- 0 until children) {

        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")

        val tp = TopicAndPartition(topic, i)

        fromOffsets += (tp -> partitionOffset.toLong)

      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD { kafkaRDD =>

      if (!kafkaRDD.isEmpty()) {
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        val lines: RDD[String] = kafkaRDD.map(_._2)

        val fields: RDD[Array[String]] = lines.map(_.split(" "))

        CalculateUtil.calculateIncome(fields)

        CalculateUtil.calculateItem(fields)

        CalculateUtil.calculateZone(fields, broadcastRef)


        for (o <- offsetRanges) {
          val zKPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          ZkUtils.updatePersistentPath(zkClient, zKPath, o.untilOffset.toString)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}

package day9

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}


import scala.concurrent.duration.Duration

object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {

    val group = "g001"

    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Duration(5000))

    val topic = "wwcc"

    val brokerList = "node-4:9092,node-5:9092,node-6:9092"

    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"

    val topics:Set[String] = Set(topic)

    val topicDirs = new ZKGroupTopicDirs(group,topic)

    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id"->group,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    val zkClient = new ZkClient(zkQuorum)

    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream:InputDStream[(String,String)] = null

    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    if(children>0){
      for(i<-0 until children){
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")

        val tp = TopicAndPartition(topic,i)

        fromOffsets += (tp ->partitionOffset.toLong)
      }

      val messageHandler = (mmd:MessageAndMetadata[String,String])=>(mmd.key(),mmd.message())

      kafkaStram = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaStream,fromOffsets, messageHandler)

    }
    else{
      kafkaStream = KafkaUtils.creteDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    }

    var offsetRanges = Array[OffsetRange]()

    val transform:DStream[(String,String)] = kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val messages:DStream[String] = tramsform.map(_._2)

    messages.foreachRDD{rdd =>
      rdd.foreachPartition(partition =>
      partition.foreach(x=>{
        println(x)
      }))
    }

    for(o <- offsetRanges){
      val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
      ZkUtils.updatePersistentPath(zkClient,zkPath,o.untilOffser.toString)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

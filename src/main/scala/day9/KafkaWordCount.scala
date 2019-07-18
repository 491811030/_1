package day9

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Second(5))

    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"

    val groupId = "g1"

    val topic: Map[String, Int] = Map[String,Int]("xiaoniuabc" ->1)

    val data: ReveciveInputDStream = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)

    val lines: DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatmap(_.split(" "))

    val wordAndOne: DStream[(String,Int)] = words.map((_,1))

    val reduce: Dstream[(String,Int)] = wordAndOne.reduceByKey(_+_)

    reduce.print()

    ssc.start()

    ssc.awaitTermination()

  }




}

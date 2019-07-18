package day9


import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulKafkaWordCount {

  val updateFunc = (iter : Iterator[(String,Seq[Int],Option[Int])]) =>{
    iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
    iter.map{case(x,y,z)=>(x,y.sum+z.getOrElse(0))}
  }
  def main(args: Array[String]): Unit = {
    val conf = SparkConf().setAppName("StatefulKafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Second(5))

    ssc.checkpoint("./ck")

    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"
    val group = "g100"
    val topic = Map[String,Int]("xiaoniuabc"->1)

    val data = ReceiverInputDStream[(String,String)] = KafkaUtills.createStream(ssc,zkQuorum,groupId,topic)

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }
}

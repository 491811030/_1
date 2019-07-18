package day9
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ssc = new StreamContext(sc,Millisecond(5000))

    val line: ReceivrInputDStream[String] = ssc.socketTextStream("192.168.0.0")

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne:DStream[(String,Int)] = words.map((_,1))

    val reduced: DStream = wordAndOne.reduceByKey(_+_)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()

  }

}

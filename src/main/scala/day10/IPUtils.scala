package day10

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object IPUtils {

  def broadcastIpRules(ssc:StreamingContext,ipRulesPath:String):Broadcast[Array[(Long,Long,String)]]={

    val sc: SparkContext = ssc.sparkContext

    val rulesLines: RDD[String] = sc.textFile(ipRulesPath)

    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")

      val startNum: Long = fields(2).toLong

      val endNum: Long = fields(3).toLong

      val province: String = fields(6)
      (startNum, endNum, province)
    })

    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    sc.broadcast(rulesInDriver)

  }

}

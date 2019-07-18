package day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("IpLocation2").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rulesLines: RDD[String] = sc.textFile(args(0))

    val ipRulesRdd: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val field: Array[String] = line.split("[|]")

      val startNum: Long = field(2).toLong

      val endNum: Long = field(3).toLong

      val province: String = field(6)
      (startNum, endNum, province)

    })
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRdd.collect()

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    val accessLines: RDD[String] = sc.textFile(args(1))

    val provinceAndOne: RDD[(String, Int)] = accessLines.map(log => {
      val fields: Array[String] = log.split("[|]")

      val ip: String = fields(1)

      val ipNum: Long = MyUtils.ip2Long(ip)

      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

      var province = "weizhi"

      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)

      if (index != -1) {
        province = rulesInExecutor(index)._3
      }

      (province, 1)


    })
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    reduced.foreachPartition(it => MyUtils.data2MySQL(it))


    sc.stop()


  }
}

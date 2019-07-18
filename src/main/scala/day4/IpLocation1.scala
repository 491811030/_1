package day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IpLocation1").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)


    val accessLines: RDD[String] = sc.textFile((args(1)))

    val func = (line:String) =>{
      val fields: Array[String] = line.split("[|]")

      val ip: String = fields(1)

      val ipNum: Long = MyUtils.ip2Long(ip)

      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

      var province = "weizhi"

      val index = MyUtils.binarySearch(rulesInExecutor,ipNum)
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    }

    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    val r: Array[(String, Int)] = reduced.collect()

    println(r.toBuffer)

    sc.stop()

  }
}

package day7

import day4.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQL2 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import spark.implicits._

    val rulesLines: Dataset[String] = spark.read.textFile(args(0))

    val rulesDataset: Dataset[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })


    val rulesInDriver: Array[(Long, Long, String)] = rulesDataset.collect()

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    val accessLine: Dataset[String] = spark.read.textFile(args(1))

    val ipDataFrame: DataFrame = accessLine.map(log => {
      val fields: Array[String] = log.split("[|]")
      val ip: String = fields(1)
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDataFrame.createTempView("v_log")

    spark.udf.register("ip2Province",(ipNum:Long)=>{
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

      val index: Int = MyUtils.binarySearch(ipRulesInExecutor,ipNum)
      var province = "weizhi"
      if(index != -1){
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    val r: DataFrame = spark.sql("SELECT ip2Province(ip_num) province,COUNT(*) count FROM v_log FROUP BY province ORDER BY counts DESC")

    r.show()

    spark.stop()


  }
}

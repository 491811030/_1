package day7

import day4.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQL {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("IpLocation").master("local[*]").getOrCreate()

    import spark.implicits._
    val rulesLines: Dataset[String] = spark.read.textFile(args(0))


    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)

    }).toDF("snum", "enum", "province")


    val accesssLines: Dataset[String] = spark.read.textFile(args(1))

    val ipDataFrame: DataFrame = accesssLines.map(log => {
      val fields: Array[String] = log.split("[|]")
      val ip: String = fields(1)

      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")


    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    val r = spark.sql("SELECT province，count(*) counts FROM v_ips JOIN v_tules ON (ip_num >= snum AND ip_num <= endnum) GROUP BY province ORDER BY countss DESC")

    r.show()

    spark.stop()



  }
}

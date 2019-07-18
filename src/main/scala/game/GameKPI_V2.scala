package game

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GameKPI_V2 {
  def main(args: Array[String]): Unit = {
    val conditionSdf = new SimpleDateFormat("yyyy-MM-dd")
    val startTime = conditionSdf.parse("2016-02-01").getTime
    val endTime = conditionSdf.parse("2016-02-02").getTime

    val filterUtils = new FilterUtils_V2 with Serializable

    val conf = new SparkConf().setAppName("GameKPI_V2").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/zx/Desktop/user.log")

    val splitedData:RDD[Array[String]] = lines.map(_.split("[|]"))
    //对数据进行过滤

    val filtered: RDD[Array[String]] = splitedData.filter(fields => {
      filterUtils.filterByTime(fields, startTime, endTime)
    })
    filtered

    val r = filtered.collect()

    println(r.toBuffer)

  }
}

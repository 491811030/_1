package game

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI_V1 {
  def main(args: Array[String]): Unit = {

    val conditionSdf = new SimpleDateFormat("yyyy-MM-dd")
    val startTime: Long = conditionSdf.parse("2016-02-01").getTime
    val endTime: Long = conditionSdf.parse("2016-02-02").getTime

    val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/zx/Desktop/user.log")

    val filteredData = lines.filter(line => {
      val fileds = line.split("[|]")
      val tp = fileds(0)
      val timeStr = fileds(1);
      //将字符串转换成Date
      val date = sdf.parse(timeStr)
      val timeLong = date.getTime
      timeLong >= startTime && timeLong < endTime
    })
    val r = filteredData.collect()

    println(r.toBuffer)






  }
}

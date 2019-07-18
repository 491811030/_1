package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val topN: Int = args(1).toInt
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher2").setMaster("loacl[4]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val subjects = Array("bigdata","javaee","php")

    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    val cacheed: RDD[((String, String), Int)] = reduced.cache()

    for(sb<-subjects){
      val filtered: RDD[((String, String), Int)] = cacheed.filter(_._1._1==sb)

      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(topN)

      println(favTeacher.toBuffer)

    }




  }

}

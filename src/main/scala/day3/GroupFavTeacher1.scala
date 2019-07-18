package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {
    val topN: Int = args(1).toInt

    val conf: SparkConf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(lines => {
      val index: Int = lines.lastIndexOf("/")
      val teacher: String = lines.substring(index + 1)

      val httpHost: String = lines.substring(0, index)
      val subject: String = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)


    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))

    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(r.toBuffer)

    sc.stop()

  }
}

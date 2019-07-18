package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FavTeacher").setMaster("local")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val teacherAndOne: RDD[(String, Int)] = lines.map(lines => {
      val index: Int = lines.lastIndexOf("/")
      val teacher: String = lines.substring(index + 1)
      (teacher, 1)
    })
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey((x:Int,y:Int)=>{x+y})


    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2)

    val result: Array[(String, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }

}

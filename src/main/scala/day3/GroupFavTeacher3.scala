package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {

    val topN: Int = args(1).toInt

    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher3").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    val sbPartitioner = new SubjectPartitioner(subjects)

    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    val r: Array[((String, String), Int)] = sorted.collect()

    println(r.toBuffer)
  }
}

class SubjectPartitioner(sbs:Array[String]) extends Partitioner{
  val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for(sb <- sbs){
    rules.put(sb,i)
    i+=1
  }

  override def numPartitions: Int = sbs.length

  override def getPartition(key: Any): Int = {

    val subject: String = key.asInstanceOf[(String,String)]._1
    rules(subject)
  }
}
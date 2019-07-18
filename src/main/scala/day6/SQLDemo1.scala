package day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQLDemo1").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines= sc.textFile("hdfs://node-4:9000/person")

    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields: Array[String] = line.split(".")
      val id = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Boy(id, name, age, fv)


    })

    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF

    bdf.registerTempTable("t_boy" )

    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv desc, age asc")

    result.show()
    sc.stop()

  }
}

case class Boy(id: Long, name: String, age: Int, fv: Double)

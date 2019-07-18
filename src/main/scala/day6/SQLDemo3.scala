package day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQLDemo3").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlCOntext = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("hdfs://node-4:9000/person")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)
    })


    val sch = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))


    val bdf: DataFrame = sqlCOntext.createDataFrame(rowRDD,sch)

    val df1: DataFrame = bdf.select("name","age","fv")

    import sqlCOntext.implicits._

    val df2: Dataset[Row] = df1.orderBy($"fv" desc,$"age" asc)

    df2.show()

    sc.stop()

  }
}

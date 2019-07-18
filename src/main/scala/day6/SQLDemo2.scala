package day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQLDEmo2").setMaster("local[2]")

    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("hedf://node-4:9000/person")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)

    })


    val sch: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD,sch)


    bdf.registerTempTable("t_boy")

    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv desc, age asc")

    result.show()

    sc.stop()

  }
}

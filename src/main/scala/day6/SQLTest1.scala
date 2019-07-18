package day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLTest1 {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("SQLTest1").master("local[*]").getOrCreate()

    val lines: RDD[String] = session.sparkContext.textFile("hdfs://node-4:9000/person")

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


    val df: DataFrame = session.createDataFrame(rowRDD,sch)

    import session.implicits._

    val df2: Dataset[Row] = df.where($"fv">98).orderBy($"fv" desc,$"age" asc)

    df2.show()


  }

}

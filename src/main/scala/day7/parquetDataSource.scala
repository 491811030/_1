package day7

import org.apache.spark.sql.{DataFrame, SparkSession}

object parquetDataSource {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("ParquetDataSource").master("local[*]").getOrCreate()

    val parquetLine: DataFrame = spark.read.parquet("/Users/zx/Desktop/parquet")

    parquetLine.printSchema()

    parquetLine.show()




  }
}

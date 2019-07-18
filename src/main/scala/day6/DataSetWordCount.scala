package day6

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DatasetWordCount").master("locla[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-4:9000/words")

    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

     val df: DataFrame = words.groupBy($"value" as "word").count()

    val r: Long = df.count()

    //导入聚合函数
    import org.apache.spark.sql.functions._
    val counts: Dataset[Row] = words.groupBy($"value".as("word")).agg(count("*") as "counts").orderBy($"counts" desc)

    counts.show()



  }
}

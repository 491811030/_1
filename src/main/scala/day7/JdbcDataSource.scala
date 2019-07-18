package day7

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("JdbcDataSource").master("local[*]").getOrCreate()

    import spark.implicits._

    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "123456"
      )
    ).load()


    val r: Dataset[Row] = logs.filter($"age" <= 13)

    val result: DataFrame = r.select($"id",$"name",$"age" * 10 as "age")

    val props = new Properties()

    props.put("user","root")
    props.put("password","123456")
    result.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata","logs1",props)

    result.write.json("/Users/zx/Desktop/text")
    result.write.text("/Users/zx/Desktop/json")
    result.write.csv("/Users/zx/Desktop/csv")
    result.write.parquet("hdfs://node-4:9000/parquet")
    result.show()
    spark.close()






  }

}

package game

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetGameKPI_2 {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val beginTime = TimeUtils(queryTime)
    //2016-02-03 00:00:00
    val endTime = TimeUtils.getCertainDayTime(3)

    val spark: SparkSession = SparkSession.builder().appName("DataSetGameKPI").master("local[*]").getOrCreate()

    import  spark.implicits._

    val lines: Dataset[String] = spark.read.textFile(args(0))

    val filterUtils = new FilterUtils_V2

    val splited: Dataset[Array[String]] = lines.map(_.split("[|]"))

    val filtered: Dataset[Array[String]] = splited.filter(fields => {
      filterUtils.filterByTime(fields, beginTime, endTime)
    })


    val updateUser: Dataset[Array[String]] = filtered.filter(field => {
      filterUtils.filterByTypes(field, EventType.REGISTER, EventType.UPGRADE)
    })
    val updateUserDF: DataFrame = updateUser.map(fields => {
      (fields(3), fields(6).toDouble)
    }).toDF("username", "level")
    updateUserDF.createTempView("v_update_user")


    val maxLevelDf: DataFrame = spark.sql("SELECT username,MAX(level) max_level  FROM v_update_user GROUP BY username")

    maxLevelDf.createTempView("v_max_level")

    val result: DataFrame = spark.sql("SELECT AVG(max_level) FROM  v_max_level")

    result.show()

    spark.stop()



  }
}

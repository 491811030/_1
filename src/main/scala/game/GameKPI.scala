package game

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI {

  def main(args: Array[String]): Unit = {

    val queryTime = "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime: Long = TimeUtils.getCertainDayTime(+1)
    val conf: SparkConf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filterUtils = new FilterUtils_V2

    val splitedLogs: RDD[Array[String]] = sc.textFile("Users/zx/Desktop/user.log").map(_.split("\\|"))

    val filteredLogs = splitedLogs.filter(fields =>{
      filterUtils.filterByTime(fields,beginTime,endTime)
    }).cache()

    val t1: Long = TimeUtils.getCertainDayTime(-1)
    val lastDayRegUser = splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields,EventType.REGISTER,t1,beginTime)).map( x => (x(3),1))


    val todayLoginUser = filteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.LOGIN))
      .map(x => (x(3), 1))
      .distinct()

    val d1r: Double = lastDayRegUser.join(todayLoginUser).count()
    println(d1r)
    val d1rr = d1r / lastDayRegUser.count()
    println(d1rr)


    sc.stop()
  }


}

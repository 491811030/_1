package day10

import day4.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {

  def calculateIncome(fields:RDD[Array[String]])={

    val priceRDD: RDD[Double] = fields.map(arr => {
      val price = arr(4).toDouble
      price
    })

    val sum: Double = priceRDD.reduce(_+_)


    val conn: Jedis = JedisConnectionPool.getConnection()

    conn.incrByFloat(Constant.TOTAL_INCOME,sum)

    conn.close()
  }

  def calculateItem(fields:RDD[Array[String]]) ={


    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {
      val item = arr(2)

      val price = arr(4).toDouble
      (item, price)
    })
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_+_)

    reduced.foreachPartition(part => {
      val conn = JedisConnectionPool.getConnection()
      part.foreach(t => {
        conn.incrByFloat(t._1,t._2)
      })

      conn.close()
    })
  }

  def calculateZone(fields:RDD[Array[String]],broadcastRef:Broadcast[Array[(Long,Long,String)]])={

    val provinceAndPrice: RDD[(String, Double)] = fields.map(arr => {

      val ip = arr(1)

      val price: Double = arr(4).toDouble
      val ipNum: Long = MyUtils.ip2Long(ip)
      val allRules: Array[(Long, Long, String)] = broadcastRef.value

      val index: Int = MyUtils.binarySearch(allRules, ipNum)

      var province = "weizhi"
      if (index != -1) {
        province = allRules(index)._3
      }
      (province, price)
    })
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)

    reduced.foreachPartition(part =>{
      val conn = JedisConnectionPool.getConnection()
      part.foreach( t => {
        conn.incrByFloat(t._1,t._2)
      })
      conn.close()
    })



  }


}

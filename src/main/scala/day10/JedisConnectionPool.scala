package day10

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

  config.setTestOnBorrow(true)

  val pool = new JedisPool(config,"192.168.1.207",6379,10000,"123")

  def getConnection():Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {

    val conn: Jedis = JedisConnectionPool.getConnection()

    val r = conn.keys("*")
    import scala.collection.JavaConversions._
    for(p <- r){
      print(p+":"+conn.get(p))
    }

  }



}

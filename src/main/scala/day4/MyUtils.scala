package day4

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}


object MyUtils {

  def ip2Long(ip:String): Long = {
    val fragments: Array[String] = ip.split("[.]")

    var ipNum = 0L

    for(i<- 0 until fragments.length){
      ipNum = fragments(i).toLong|ipNum<<8L
    }
    ipNum
  }

  def readRules(path:String ):Array[(Long,Long,String)] = {

    val bf: BufferedSource = Source.fromFile(path)

    val lines: Iterator[String] = bf.getLines()

    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)

    }).toArray
    rules
  }

  def binarySearch(lines:Array[(Long,Long,String)],ip:Long):Int = {
    var low = 0
    var high = lines.length-1

    while(low<=high){
      val middle = (low+high)/2
      if((ip>=lines(middle)._1)&&(ip<=lines(middle)._2))
        return middle
      if(ip<lines(middle)._1)
        high = middle-1
      else{
        low=middle+1
      }
    }

    -1
  }


  def data2MySQL(it:Iterator[(String,Int)]):Unit = {
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123568")

    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")

    it.foreach(tp => {
      pstm.setString(1,tp._1)
      pstm.setInt(2,tp._2)
      pstm.executeUpdate()
    })

    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    //数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("/Users/zx/Desktop/ip/ip.txt")
    //将ip地址转换成十进制
    val ipNum = ip2Long("114.215.43.42")
    //查找
    val index = binarySearch(rules, ipNum)
    //根据脚本到rules中查找对应的数据
    val tp = rules(index)
    val province = tp._3
    println(province)
  }
}

package ser

import java.net.InetAddress

object Rules extends Serializable {
  val rulesMap = Map("hadoop" -> 2.7,"spark"->2.2)

  val hostname = InetAddress.getLocalHost.getHostName

  println(hostname+"@@@@")

}


object Rules{
  val rulesMap = Map("hadoop" -> 2.7,"spark"->2.2)

  val hostname = InetAddress.getLocalHost.getHostName

  println(hostname + "@@@@@")
}

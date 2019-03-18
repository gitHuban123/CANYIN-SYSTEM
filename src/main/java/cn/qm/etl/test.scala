package cn.qm.etl

object test {
  def main(args: Array[String]): Unit = {
    val s="ss11==("
   "^[0-9]+".r.findAllIn(s).toList.foreach(println(_))
//    val numpattern ="""\d+""".r
//
//     println(numpattern.findAllIn("99 a,98 b").toArray)
  }
}

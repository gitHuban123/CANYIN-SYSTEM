package cn.qm.etl

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ETLDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sss").setMaster("local")
    val sc = new SparkContext(conf)
    val sourcerdd=sc.textFile("C:\\Users\\Administrator\\Desktop\\test.txt")
    val accumulator: Accumulator[String] = sc.accumulator("---")(MySessionAccumulator)
    sourcerdd.map(line=>{
      accumulator.add(line)
    }).collect()
    val result1=accumulator.value
    val result=result1.substring(1,result1.size)
    var list=new ListBuffer[bean]()
    result.split("\\|").foreach(f=>{
//      println(f)
      var pub_pac_arr=f.split("<>")
//      println(pub_pac_arr.size)
      if(pub_pac_arr.size>1){
        val pub_arr=pub_pac_arr(0).split(" ")
        pub_pac_arr(1).split(";").foreach(f=> {
          println("f="+f)
          if (f.contains("com.tcl.thirdAppPlayBehavior")) {
            val info = pub_arr(6)
            val bean = new bean()
            bean.client_type = info.split("&")(3).split("=")(1)
            bean.mac_line = info.split("&")(6).split("=")(1)
            val pac_arr = pub_pac_arr(1).split(",")
            bean.package_name = pac_arr(0)
            bean.origin = pac_arr(1).substring(1, pac_arr(1).length)
            //duration,origin,version,total_duration,dt
            bean.program_name = pac_arr(3)
            bean.total_duration = pac_arr(4)
            bean.duration = pac_arr(5).substring(0, pac_arr(5).size - 1)
//            println(bean)
            list += bean
          } else if (f.contains("com.ktcp.video")) {
            val info = pub_arr(6)
            val bean = new bean()
//            println("info="+info)
            bean.client_type = info.split("&")(3).split("=")(1)
            bean.mac_line = info.split("&")(6).split("=")(1)
            val pac_arr = pub_pac_arr(1).split(",")
            bean.package_name = pac_arr(0)
            bean.start_time = pac_arr(1).substring(1, pac_arr(1).length())
            bean.end_time = pac_arr(2)
            list += bean
          }

      })
      }

    })
      sc.parallelize(list).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\test2")

  }

}

package cn.qm.etl

import org.apache.spark.AccumulatorParam

object MySessionAccumulator extends AccumulatorParam[String]{
  override def addInPlace(r1: String, r2: String): String = {
    if(r1=="---"){
      r2
    }else{
      val flag1:Boolean="<>$".r.findAllIn(r2).toList.size>0
      val flag2:Boolean="^[0-9]".r.findAllIn(r2).toList.size>0
      if(true==flag2){
        r1.concat("|"+r2+"<>")
      }else{
        r1.concat(r2+";")
      }
    }
  }

  override def zero(initialValue: String): String = {
    ""
  }
}

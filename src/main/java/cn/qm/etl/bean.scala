package cn.qm.etl

class bean extends Serializable {
  //client_type,mac_line,package_name,start_time,end_time,program_name,duration,origin,version,total_duration,dt
  var client_type:String=""
  var mac_line:String=""
  var package_name:String=""
  var start_time:String=""
  var end_time:String=""
  var duration:String=""
  var origin:String=""
  var version:String=""
  var total_duration:String=""
  var program_name:String=""


  override def toString = s"$client_type, $mac_line, $package_name, $start_time, $end_time, $duration, $origin, $version, $total_duration, $program_name"
}

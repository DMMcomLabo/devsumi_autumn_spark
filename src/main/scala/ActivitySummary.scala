
package com.dmm.i3.summary

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat

import scala.collection.mutable.ArrayBuffer 

object ActivitySummary{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ActivitySummary")
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaStream = KafkaUtils.createStream(ssc,"172.16.100.180:2181,172.16.100.181:2181,172.160.100.182:2181","ActivitySummary",Map("raw_tracking"->3))
    //中間データ
    val middle_data = kafkaStream
    .map(line => parseRecode(line))
    .map(fields => parseColumns(fields))
    .flatMap(d =>{
       var open_id:String = ""
       var action:String = ""
       var i3_service_code:String = ""
       var cookie = ""
       d.foreach(column =>{
         if(column._1 == "i3_service_code") i3_service_code= column._2
         if(column._1 == "action") action = column._2
         if(column._1 == "cookie") cookie= column._2
         if(column._1 == "open_id") open_id = column._2
       })
       val list = ArrayBuffer.empty[(String,Int)]
       //all系
       list += (("all_pv",1))  //全てのPV
       list += ((action + "_all_pv",1))  //action別PV
       list += ((i3_service_code + "_pv",1))  //事業部(i3_service_code)別PV
       list += (("all_uu_"+cookie,1)) //cookieでのuu

       //会員系
       if(open_id != "") list += (("member_pv_"+open_id,1)) //全ての会員pv
       if(open_id != "") list += ((i3_service_code + "_member_pv_"+open_id,1)) //事業部(i3_service_cde)別会員pv
       if(open_id != "") list += ((action + "_member_pv_"+open_id,1)) //アクション別会員pv

       if(open_id != "") list += (("member_uu_"+open_id,1)) //全ての会員uu
       if(open_id != "") list += ((i3_service_code + "_member_uu_"+open_id,1)) //事業部(i3_service_cde)別会員uu
       if(open_id != "") list += ((action+ "_member_uu_"+open_id,1)) //アクション別会員uu

       //非会員系
       if(open_id == "" && cookie != "" ) list += (("unmember_pv_"+cookie,1)) //全ての非会員pv
       if(open_id == "" && cookie != "" ) list += ((i3_service_code + "_unmember_pv_"+cookie,1)) //事業部(i3_service_code)別非会員pv
       if(open_id == "" && cookie != "" ) list += ((action + "_unmember_pv_"+cookie,1)) //アクション別非会員pv

       if(open_id == "" && cookie != "" ) list += (("unmember_uu_"+cookie,1)) //全ての非会員uu
       if(open_id == "" && cookie != "" ) list += ((i3_service_code + "_unmember_uu_"+cookie,1)) //事業部(i3_service_code)別非会員uu
       if(open_id == "" && cookie != "" ) list += ((action + "_unmember_uu_"+cookie,1)) //アクション別別非会員uu

       list
    })

    //中間データからuuの重複排除
    middle_data.filter(_._1.contains("_uu_"))
    .map(x => (x._1,1))
//    .countByValue()
    .print()

//    middle_data.reduceByKey(_+_)




//    .map(x =>{
//      val hConf = HBaseConfiguration.create()
//      hConf.set("hbase.zookeeper.quorum","172.16.100.180:2181,172.16.100.181:2181,172.16.100.182:2181")
//      hConf.set("hbase.master","172.16.100.180:60000")
//      hConf.set("hbase.rootdir","/hbase")
//      val hTable = new HTable(hConf,"h_summary")
//      val row = new Put(Bytes.toBytes("test"))
//      row.add(Bytes.toBytes("api"),Bytes.toBytes("test_key"),Bytes.toBytes("test_value"))
//      hTable.put(row)
//    })
     
    sys.ShutdownHookThread{
      System.out.println("Gracefully stopping ActivitySummary")
      ssc.stop(true,true)
      System.out.println("ActivitySummary stopped")
    }

    ssc.start()
    ssc.awaitTermination()
  }
  //def summary(data: (String,String)):String = {
  //   val (key,value) = data
  //}
  def parseRecode(data: (String,String)):Array[String] = {
    val (key,v) = data
    v.split(",")
  }
  def parseColumns(data: Array[String]):Array[(String,String)] = {
    var d = ""
    data.map(r =>{
      val index = r.indexOf(":")
      val (key,value) = r.splitAt(index)
      (key,value.slice(1,value.size))
    })
  }
}


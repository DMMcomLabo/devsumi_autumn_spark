
package com.dmm.i3.logging

import java.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat

import scala.collection.mutable.ArrayBuffer 


object TransposeActivityData{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("TransposeActivityData")
    val ssc = new StreamingContext(conf,Seconds(1))

    val kafkaStream = KafkaUtils.createStream(ssc,"172.27.100.11:2181,172.27.100.12:2181,172.27.100.13:2181","TransposeActivityData",Map("raw_tracking"->9))
    //中間データ
    val base_data = kafkaStream
    .map(line => parseRecode(line))
    .map(fields => parseColumns(fields))
    val middle_data = base_data
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
       list += (("all_pv_",1))  //全てのPV
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
    var uu = middle_data
    .filter(_._1.contains("_uu_"))
    .map(x => x._1)
    .transform(rdd => rdd.distinct())
    .map(x => x.substring(0,x.lastIndexOf("_")))
    .countByValue()

    val pv = middle_data
    .filter(_._1.contains("_pv_"))
    .map(x => x._1)
    .map(x => x.substring(0,x.lastIndexOf("_")))
    .countByValue()


    pv
    .union(uu)
    .transform(rdd =>{
      val t = (Long.MaxValue-System.currentTimeMillis)
      rdd.map(x =>{ (x._1,x._2,t)})
    })
    .map(x =>{
      val hConf = HBaseConfiguration.create()
      hConf.set("hbase.zookeeper.quorum","172.27.100.11:2181,172.27.100.12:2181,172.27.100.13:2181")
      hConf.set("hbase.master","172.27.100.11:60000")
      hConf.set("hbase.rootdir","/hbase")
      val hTable = new HTable(hConf,"h_summary")
      val row = new Put(Bytes.toBytes(x._3.toString))
      val cF = if(x._1.contains("_uu_")) "summary_uu" else "summary_pv"
      row.add(Bytes.toBytes(cF),Bytes.toBytes(x._1),Bytes.toBytes(x._2.toString))
      hTable.put(row)
      x
    })
    .foreachRDD(rdd=>{
      var time:String = ""
      val msg:String = rdd.map(x =>{
        if(time.length != 0) time = x._3.toString
        x._1 +"::"+ x._2
      })
      .collect
      .mkString(time,",","")
      val props:Properties = new Properties()
      props.put("metadata.broker.list","172.27.100.14:9092,172.27.100.15:9092,172.27.100.16:9092,172.27.100.17:9092,172.27.100.18:9092,172.27.100.19:9092,172.27.100.20:9092,172.27.100.21:9092,172.27.100.22:9092,172.27.100.29:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("request.required.acks", "1")
      val config:ProducerConfig = new ProducerConfig(props)
      val producer:Producer[String,String] = new Producer[String,String](config)
      val data:KeyedMessage[String,String] = new KeyedMessage[String,String]("tracking_summary",msg)
      producer.send(data)
      producer.close
    })


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


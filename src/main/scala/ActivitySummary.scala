
package com.dmm.i3.summary

import java.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
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


object ActivitySummary{
  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("ActivitySummary")
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaParams = Map[String,String]("metadata.broker.list" -> "172.27.100.14:9092,172.27.100.15:9092,172.27.100.16:9092,172.27.100.17:9092,172.27.100.18:9092,172.27.100.19:9092,172.27.100.20:9092,172.27.100.21:9092,172.27.100.22:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("raw_tracking"))
//    val kafkaStream =KafkaUtils.createStream(ssc,"172.27.100.11:2181,172.27.100.12:2181,172.27.100.13:2181","ActivitySummary",Map("raw_tracking"->9))
//=====バグふむパティーン
//    val kafkaStream = KafkaUtils.createDirectStream(ssc,kafkaParams,Set("raw_tracking")).print

    //parseしたデータセット
    val base_data = kafkaStream
    .map(line => parseRecode(line))
    .map(fields => parseColumns(fields))

    //中間データ
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
//    .checkpoint(Seconds(60))
    
    //中間データからuuの重複排除
    var uu = middle_data
    .filter(_._1.contains("_uu_"))
    .map(x => x._1)
    .transform(rdd => rdd.distinct())
    .map(x => x.substring(0,x.lastIndexOf("_")))
    .countByValue()

    //中間データからPVを数えるよ！
    val pv = middle_data
    .filter(_._1.contains("_pv_"))
    .map(x => x._1.substring(0,x._1.lastIndexOf("_")))
    .countByValue()
    
    //pvの移動平均用
    val window_pv = middle_data
    .filter(_._1.contains("_pv_"))
    .map(x => "window_" + x._1.substring(0,x._1.lastIndexOf("_")))
    .countByValueAndWindow(Seconds(60),Seconds(5))


    pv
    .union(uu)
    .transform(rdd =>{
      val t = (Long.MaxValue-System.currentTimeMillis)
      rdd.map(x =>{ (x._1,x._2,t)})
    })
    .map(x =>writeToActivitySummary(x))
    .map(x=>(x._3,x._1 + "::" + x._2))
    .groupByKey
    .foreachRDD(rdd=>{
      val msg:String = rdd.map(x => ("spark-time::"+x._1,x._2.mkString(",")))
      .filter(_._2.length != 0)
      .collect
      .mkString(",")
      .replaceAll("\\(","").replaceAll("\\)","")
      writeToKafka(msg)
      printf(msg)
      
    })
    sys.ShutdownHookThread{
      System.out.println("Gracefully stopping ActivitySummary")
      ssc.stop(true,true)
      System.out.println("ActivitySummary stopped")
    }

    ssc.checkpoint("/tmp/com/dmm/i3/summary/ActivitySummary-checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
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
  def writeToActivitySummary(data:(String,Long,Long)):(String,Long,Long) = {
      val cF = if(data._1.contains("_uu_")) "summary_uu" else "summary_pv"
      HBaseWriter.getInstance().write(data._3.toString,cF,data._1,data._2.toString)
      data
  }
  def writeToKafka(msg:String) : String ={
    if(msg.length != 0) KafkaProducer.getInstance().send("tracking_summary",msg)
//    producer.close
    msg
  }
}
class HBaseWriter private(zkList:String,master:String){
  val hConf = HBaseConfiguration.create()
  hConf.set("hbase.zookeeper.quorum",zkList)
  hConf.set("hbase.master",master)
  hConf.set("hbase.rootdir","/hbase")
  val hTable = new HTable(hConf,"h_summary")
  def write(key:String,cf:String,c:String,v:String){
    val row = new Put(Bytes.toBytes(key))
    row.add(Bytes.toBytes(cf),Bytes.toBytes(c),Bytes.toBytes(v))
    hTable.put(row)
  }
  def close(){}
}
object HBaseWriter{
  private val hbWriter = new HBaseWriter("172.27.100.11:2181,172.27.100.12:2181,172.27.100.13:2181","172.27.100.11:60000")
  def getInstance():HBaseWriter = { hbWriter }
  def apply():HBaseWriter ={ getInstance }
}
class KafkaProducer private(brokerList:String){
  val props:Properties = new Properties()
  props.put("metadata.broker.list",brokerList)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  val config:ProducerConfig = new ProducerConfig(props)
  val producer:Producer[String,String] = new Producer[String,String](config)
  def send(topicName:String,msg:String){
    val data:KeyedMessage[String,String] = new KeyedMessage[String,String](topicName,msg)
    producer.send(data)
  }
  def close(){}
}
object KafkaProducer{
  private val kProducer = new KafkaProducer("172.27.100.14:9092,172.27.100.15:9092,172.27.100.16:9092,172.27.100.17:9092,172.27.100.18:9092,172.27.100.19:9092,172.27.100.20:9092,172.27.100.21:9092,172.27.100.22:9092,172.27.100.29:9092")
  def getInstance():KafkaProducer ={ kProducer }
  def apply():KafkaProducer ={ getInstance }
}


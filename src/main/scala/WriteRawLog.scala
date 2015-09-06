
package com.dmm.i3.logUtils

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


object WriteRawLog{
  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("WriteRawLog")
    val ssc = new StreamingContext(conf,Seconds(1))

    val kafkaParams = Map[String,String]("metadata.broker.list" -> "172.27.100.14:9092,172.27.100.15:9092,172.27.100.16:9092,172.27.100.17:9092,172.27.100.18:9092,172.27.100.19:9092,172.27.100.20:9092,172.27.100.21:9092,172.27.100.22:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("raw_tracking"))

    //parseしたデータセット
    val base_data = kafkaStream
    .map(line => parseRecode(line))
    .map(fields => parseColumns(fields))

    //ColumnFamilyとのMapping
    val middle_data = base_data
    .map(d =>{
       val i3_service_code = d.filter(_._1 == "i3_service_code")
       (i3_service_code(0)._2,d)
    })
    .repartition(90)
    .foreachRDD(rdd => {
      rdd.foreachPartition(x=>{
        val ReverseTimestamp = (Long.MaxValue-System.currentTimeMillis).toString
        x.foreach(row =>{
          writeToActivityExt((row._1+"_"+ReverseTimestamp,row._2))
        })
      })
    })
    
    sys.ShutdownHookThread{
      System.out.println("Gracefully stopping WriteRawLog")
      ssc.stop(true,true)
      System.out.println("WriteRawLog stopped")
    }

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
  def writeToActivityExt(data:(String,Array[(String,String)])) = {
      data._2.map(columns => {
        val cF = HBaseWriter.getInstance().getColumnFamily(columns._1)
        HBaseWriter.getInstance().write(data._1,cF,columns._1,columns._2)
      })
  }
}
class HBaseWriter private(zkList:String,master:String){
  val v = Map[String,String]("i3_service_code"->"base",
            "log_date"->"base",
            "action"->"base",
            "option"->"base",
            "open_id"->"base",
            "session"->"base",
            "cookie"->"base",
            "view_type"->"base",
            "user_agent"->"base",
            "ua_replace_id"->"base",
            "url"->"base",
            "url_replace_id"->"base",
            "time"->"base",
            "ip"->"base",
            "segment"->"base",
            "referer"->"view",
            "referer_replace_id"->"view",
            "referer_page_id"->"view",
            "randing_flg"->"view",
            "randing_type"->"view",
            "hit_count"->"api",
            "page_count"->"api",
            "view_condition"->"api",
            "content_ids"->"api",
            "product_ids"->"api",
            "uids"->"api",
            "prices"->"api",
            "stocks"->"api",
            "api_type"->"api",
            "via_module"->"api",
            "parameter"->"api",
            "word"->"api",
            "category"->"api",
            "sort"->"api",
            "view_pattern"->"api",
            "re_search_flg"->"api",
            "api_response_time"->"api",
            "client_response_time"->"api",
            "content_id"->"detail",
            "content_ids"->"detail",
            "product_ids"->"detail",
            "price"->"detail",
            "stock_flg"->"detail",
            "via_info"->"detail",
            "via_info_options"->"detail",
            "tran_view_order"->"detail",
            "via_module"->"detail",
            "month_service_id"->"detail",
            "month_service_flg"->"detail",
            "month_service_end_date"->"detail",
            "month_service_price"->"detail",
            "browser_lang"->"user",
            "contry_code"->"user",
            "region"->"user",
            "city"->"user",
            "latitude"->"user",
            "longitude"->"user",
            "purchase_id"->"purchase",
            "purchase_id_type"->"purchase",
            "sites"->"purchase",
            "espdb_categorys"->"purchase",
            "content_ids"->"purchase",
            "product_ids"->"purchase",
            "uids"->"purchase",
            "prices"->"purchase",
            "numbers"->"purchase",
            "via_infos"->"purchase",
            "via_info_options"->"purchase",
            "subtotal"->"purchase",
            "tax"->"purchase",
            "total"->"purchase",
            "purchase_type"->"purchase",
            "month_service_id"->"purchase",
            "month_service_end_date"->"purchase"
            )
  val hConf = HBaseConfiguration.create()
  hConf.set("hbase.zookeeper.quorum",zkList)
  hConf.set("hbase.master",master)
  hConf.set("hbase.rootdir","/hbase")
  val hTable = new HTable(hConf,"h_activity_ext")
  def getColumnFamily(c:String):String = { v.getOrElse(c,"base") }
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

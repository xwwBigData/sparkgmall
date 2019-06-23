package com.atguigu.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.bean.KafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


// 广告点击量实时统计
object Req5DateAreaCityAdvCountApplication {

  def main(args: Array[String]): Unit = {

    // 创建Spark流式数据处理环境e
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req5DateAreaCityAdvCountApplication")
        val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    // TODO 4.1 从kafka中周期性获取广告点击数据
    val topic = "ads_log181111"
    //
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)
    // TODO 4.2 将数据进行分解和转换：（ts_user_ad, 1）
    val messageDstream: DStream[KafkaMessage] = kafkaDstream.map(record => {
      val massage: String = record.value()
      val massages: Array[String] = massage.split(" ")
      KafkaMessage(massages(0), massages(1), massages(2), massages(3), massages(4))
    })

    val mapDstream: DStream[(String, Int)] = messageDstream.map(message => {
      val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
      (dateString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
    })

    // 将采集周期中的数据进行聚合
    streamingContext.sparkContext.setCheckpointDir("cp")

    val stateDStream: DStream[(String, Long)] = mapDstream.updateStateByKey {
      case (seq, opt) => {
        val sum = seq.sum + opt.getOrElse(0l)
        Option(sum)
      }
    }
   /* stateDStream.foreachRDD(rdd=>{
      rdd.foreach(println)
    })*/
    // 将聚合后的数据更新到redis中
    stateDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val jedis: Jedis = RedisUtil.getJedisClient
        for (data <-datas){
          val key = "date:area:city:ads"
          jedis.hset(key,data._1,""+data._2)
        }
        jedis.close()
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

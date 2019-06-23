package com.atguigu.sparkmall.realtime

import java.util
import java.util.Set

import com.atguigu.bigdata.sparkmall.common.bean.KafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 广告黑名单实时统计
/*
将每天对某个广告点击超过 100 次的用户拉黑。
注：黑名单保存到redis中。
已加入黑名单的用户不在进行检查。
*/

object Req4RedisUserBlackListApplication {

  def main(args: Array[String]): Unit = {

    // 创建Spark流式数据处理环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4RedisUserBlackListApplication")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // TODO 4.1 从kafka中周期性获取广告点击数据
    val topic = "ads_log181111"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    // TODO 4.2 将数据进行分解和转换：（ts_user_ad, 1）
    val messageDstream: DStream[KafkaMessage] = kafkaDstream.map(record => {
      val message: String = record.value()
      val messages: Array[String] = message.split(" ")
      val ts: String = SparkmallUtil.parseStringDateFromTs(messages(0).toLong, "yyyy-MM-dd")
      KafkaMessage(ts, messages(1), messages(2), messages(3), messages(4))
    })

    //fliter 这里用到了transform周期性获得数据，broadcast 广播数据到excutor
    val filterDStream: DStream[KafkaMessage] = messageDstream.transform(rdd => {
      val jedis: Jedis = RedisUtil.getJedisClient
      val blacklist: Set[String] = jedis.smembers("blacklist")
      val broadcastSet: Broadcast[ Set[String]] = streamingContext.sparkContext.broadcast(blacklist)
      rdd.filter(message => {
        !broadcastSet.value.contains(message.userid)
      }
      )
    })

    //转化成（ts_user_ad, 1）
    val mapDstream: DStream[(String, Int)] =  filterDStream.map(data => {
      (data.ts + "_" + data.userid + "_" + data.adid, 1)
    })
    //有状态聚合
    streamingContext.sparkContext.setCheckpointDir("cp")

    //stateDstream 里面是一个个rdd,
    val stateDstream: DStream[(String, Long)] = mapDstream.updateStateByKey {
      case (seq, qut) => {
        val sum = seq.sum + qut.getOrElse(0l)
        Option(sum)
      }
    }

    stateDstream.foreachRDD(rdd => {
      rdd.foreach {
        case (key, sum) => {
          val userID: String = key.split("_")(1)
          if (sum > 50) {
            val jedis = new Jedis("192.168.1.106", 6379)
            jedis.sadd("blacklist", userID)
          }
        }
      }
    })

    //      stateDstream.foreachRDD(rdd=>{
    //        rdd.foreach{
    //          case ( key, sum ) => {
    //            if ( sum > 50 ) {
    //              // TODO 4.6 如果超过阈值，那么将用户加入黑名单，防止用户继续访问
    //              // 将数据更新到redis中
    //              //val jedis: Jedis = RedisUtil.getJedisClient
    //              val jedis : Jedis = new Jedis("linux4", 6379)
    //
    //              // 获取用户
    //              val userid = key.split("_")(1)
    //
    //              // redis : blackList
    //              jedis.sadd("blacklist",userid)
    //
    //            }
    //          }
    //        }
    //      })

    //messageDStream.print()
    // 将获取的数据进行筛选（过滤掉黑名单的数据）
    /*
    val filterDStream: DStream[KafkaMessage] = messageDStream.filter(message => {
        // 从redis中获取黑名单
        !blackListSet.contains(message.userid)
    })
    */


  }
}

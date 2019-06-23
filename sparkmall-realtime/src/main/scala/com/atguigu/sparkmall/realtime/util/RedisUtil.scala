package com.atguigu.sparkmall.realtime.util

import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
    var jedisPool:JedisPool=null

    def getJedisClient: Jedis = {
        if(jedisPool==null){
            //println("开辟一个连接池")
            val host = SparkmallUtil.getValueFromConfig("redis.host")
            val port = SparkmallUtil.getValueFromConfig("redis.port")


            val jedisPoolConfig = new JedisPoolConfig()
            jedisPoolConfig.setMaxTotal(200)  //最大连接数
            jedisPoolConfig.setMaxIdle(50)   //最大空闲
            jedisPoolConfig.setMinIdle(8)     //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(10000)//忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

            jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
        }
        //println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
        //println("获得一个连接")
        jedisPool.getResource
    }

}

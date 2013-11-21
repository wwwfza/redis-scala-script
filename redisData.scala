#!/bin/env scala
import java.util.zip.CRC32
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisPool, BinaryJedis}
import scala.io.Source
import java.io.PrintWriter

val divided = 8
val recordCount = 100000
val redisHost = "localhost"
val redisPort = 6379
val appKey = "5eee98145270153fea000003"

def mod(str: String): Int = {
    val res = crc32(str) % divided
    res.toInt
  }

def crc32(str: String): Long = {
    var hasher = new CRC32()
    hasher.update(str.getBytes())
    hasher.getValue
  }

def generarteRandomString(StringLength:Int) = {
    val indexList = scala.util.Random.shuffle((0 until charList.length).toList)
    var s:String = ""
    for (i <- 1 to StringLength)
    {
	s+=charList(indexList(i))
    }
    s
}

def updateRedis(index:Int) = {
    val jedis = pool.getResource()
    val alias = generarteRandomString(8)
    val key = "%s::%s".format(appKey,alias)
    val value = generarteRandomString(6)
    val db = mod(key)
    jedis.select(db)
    if(jedis.exists(key)){
        val valSet = Set(jedis.get(key).split(","): _*)
        if(!valSet(value))
          jedis.append(key, ","+value)
      }
      else{
        jedis.set(key,value)
      }
    pool.returnResource(jedis.asInstanceOf[BinaryJedis])
    out.println(alias)
    println("""record %d into db %d: {"%s":"%s"}""".format(index,db,key,value))
}

val pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort,0)
val out = new PrintWriter("alias.txt")
val charList = ('a' to 'z').toList
(1 to recordCount).foreach(updateRedis)
out.close()

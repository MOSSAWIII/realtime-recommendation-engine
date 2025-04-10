package core

import models._
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import com.typesafe.config.ConfigFactory
import java.time.{LocalDateTime, Duration}
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import io.circe.{Encoder, Decoder, Json}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

class CacheManager(implicit ec: ExecutionContext) {
  private val config = ConfigFactory.load()
  private val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(config.getInt("redis.pool.maxTotal"))
  poolConfig.setMaxIdle(config.getInt("redis.pool.maxIdle"))
  poolConfig.setMinIdle(config.getInt("redis.pool.minIdle"))
  
  private val jedisPool = new JedisPool(
    poolConfig,
    config.getString("redis.host"),
    config.getInt("redis.port"),
    config.getInt("redis.timeout"),
    config.getString("redis.password")
  )
  
  private val cacheExpirySeconds = config.getInt("redis.cache.expirySeconds")
  
  def withJedis[T](operation: Jedis => T): Future[T] = Future {
    val jedis = jedisPool.getResource
    try {
      operation(jedis)
    } finally {
      jedis.close()
    }
  }
  
  def cacheUser(user: User): Future[Boolean] = {
    withJedis { jedis =>
      val key = s"user:${user.userId}"
      val result = jedis.setex(key, cacheExpirySeconds, user.asJson.noSpaces)
      result == "OK"
    }
  }
  
  def getUser(userId: Long): Future[Option[User]] = {
    withJedis { jedis =>
      Option(jedis.get(s"user:$userId")).flatMap { json =>
        decode[User](json).toOption
      }
    }
  }
  
  def cacheEvent(event: Event): Future[Boolean] = {
    withJedis { jedis =>
      val key = s"event:${event.eventId}"
      val result = jedis.setex(key, cacheExpirySeconds, event.asJson.noSpaces)
      result == "OK"
    }
  }
  
  def getEvent(eventId: Long): Future[Option[Event]] = {
    withJedis { jedis =>
      Option(jedis.get(s"event:$eventId")).flatMap { json =>
        decode[Event](json).toOption
      }
    }
  }
  
  def cacheRecommendation(userId: Long, events: List[Event]): Future[Boolean] = {
    withJedis { jedis =>
      val key = s"rec:$userId"
      val result = jedis.setex(key, cacheExpirySeconds, events.asJson.noSpaces)
      result == "OK"
    }
  }
  
  def getRecommendation(userId: Long): Future[Option[List[Event]]] = {
    withJedis { jedis =>
      Option(jedis.get(s"rec:$userId")).flatMap { json =>
        decode[List[Event]](json).toOption
      }
    }
  }
  
  def invalidateUser(userId: Long): Future[Boolean] = {
    withJedis { jedis =>
      jedis.del(s"user:$userId") > 0
    }
  }
  
  def invalidateEvent(eventId: Long): Future[Boolean] = {
    withJedis { jedis =>
      jedis.del(s"event:$eventId") > 0
    }
  }
  
  def invalidateUserRecommendations(userId: Long): Future[Boolean] = {
    withJedis { jedis =>
      jedis.del(s"rec:$userId") > 0
    }
  }
  
  def clear(): Future[Boolean] = {
    withJedis { jedis =>
      jedis.flushDB() == "OK"
    }
  }
  
  def close(): Unit = {
    jedisPool.close()
  }
}


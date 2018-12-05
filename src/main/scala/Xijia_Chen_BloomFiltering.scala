import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import twitter4j.Status

import scala.util.Random
import java.security.MessageDigest
import java.util.BitSet

import scala.util.hashing.MurmurHash3




object Xijia_Chen_BloomFiltering {
  def main(args: Array[String]): Unit = {
    val consumerKey = "Kw6Ki3Cykna5hrUltPx4wDbbi"
    val consumerSecret="F7ZYIBCXWeWgv8Bqcl2QX8FjbVzRKbXoouyW0PiDUgIPIgLuSl"
    val accessToken="3471172398-BxJoA7p7oYmZFHXGYRUu6SkyzDtmRbbxMJsJ0LO"
    val accessTokenSecret="Af6765ZltIbJCFuO2DW2mnhsiFy82euAFjjm0xGruV2Bu"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sc = new SparkContext(new SparkConf().setAppName("554hw5_task2").setMaster("local[2]"))
    sc.setLogLevel(logLevel = "OFF")
    val ssc = new StreamingContext(sc,Seconds(10))
//    val ssc = new StreamingContext(sparkConf, Seconds(10))

//    var Bitmap = new Array[Int](256)
    val bitSetSize = 1 << 12
    val Bitmap = new BitSet(bitSetSize)
    var pre_tw = Set.empty[String]
    var N = 0
    var bf_pos = 0
    var real_pos = 0

    def findtag(hashval: Array[Int] ): Boolean = {
      var exist = true
      for(i <- 0 until hashval.length){
        if (!Bitmap.get(hashval(i))){
          exist = false
          Bitmap.set(hashval(i),true)
        }
      }
      exist
    }

    def BloomFiltering(tw_rdd : RDD[Status]) : Unit = {
      var tws = tw_rdd.collect().toList
      for (tw <- tws){
        var tags = tw.getHashtagEntities().map(x=>(x.getText))
        for (tag<-tags){
          N += 1
          if (pre_tw.contains(tag)){real_pos+=1}
          else{pre_tw += tag}

//          var hashval = MessageDigest.getInstance("MD5").digest(tag.getBytes)
          var hash1 = MurmurHash3.stringHash(tag, 1)
          var hash2 = MurmurHash3.stringHash(tag, 2)
          var hash3 = MurmurHash3.stringHash(tag, 3)
          var hash4 = MurmurHash3.stringHash(tag, 4)
          var hash5 = MurmurHash3.stringHash(tag, 5)
          var hashval = Array[Int](hash1%5000+5000, hash2%5000+5000, hash3%5000+5000,hash4%5000+5000, hash5%5000+5000)
          if(findtag(hashval)==true){
            bf_pos +=1
          }
        }
      }
      println("The number of the tags from beginning: " + N)
      println("The number of real previously appeared tags from beginning: " + real_pos)
      println("The number of BF-predicted previously appeared tags from beginning: " + bf_pos)
      println("The number of correct estimation: " + real_pos)
      println("The number of incorrect estimation: " + (bf_pos-real_pos))
      println("False positive rate: " + (bf_pos-real_pos).toDouble/N.toDouble)
      println()
      println()
      println()
    }


    val stream = TwitterUtils.createStream(ssc, None)

    stream.foreachRDD(rdd =>BloomFiltering(rdd))
    ssc.start()
    ssc.awaitTermination()


  }
}

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


object Xijia_Chen_TwitterStreaming {
  def main(args: Array[String]): Unit = {

    val consumerKey = "Kw6Ki3Cykna5hrUltPx4wDbbi"
    val consumerSecret="F7ZYIBCXWeWgv8Bqcl2QX8FjbVzRKbXoouyW0PiDUgIPIgLuSl"
    val accessToken="3471172398-BxJoA7p7oYmZFHXGYRUu6SkyzDtmRbbxMJsJ0LO"
    val accessTokenSecret="Af6765ZltIbJCFuO2DW2mnhsiFy82euAFjjm0xGruV2Bu"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sc = new SparkContext(new SparkConf().setAppName("554hw5_task1").setMaster("local[2]"))
    sc.setLogLevel(logLevel = "OFF")
    val ssc = new StreamingContext(sc,Seconds(10))

    //popular tags
//    val stream = TwitterUtils.createStream(ssc, None)
//    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
//    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//      .map{case (topic, count) => (count, topic)}
//      .transform(_.sortByKey(false))
//    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//      .map{case (topic, count) => (count, topic)}
//      .transform(_.sortByKey(false))
//
//    topCounts60.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })
//
//    topCounts10.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
    var S = 100
    var N = 0
    var alllength = 0
    var sample_list = new ListBuffer[Status]()
    var tag_map = scala.collection.mutable.Map[String, Int]()

    def sample_func(tw_rdd : RDD[Status]) : Unit = {
      var tws = tw_rdd.collect().toList
      for (tw <- tws){
        if(N < S){
          sample_list.append(tw)
          var tags = tw.getHashtagEntities().map(x=>(x.getText))
          for(tag <- tags){
            if(tag_map.contains(tag)){tag_map(tag)+=1}
            else{tag_map(tag) = 1}
          }
          alllength += tw.getText().length
          N+=1
        }else{
          N+=1
          val ran = Random.nextInt(N)
          if(ran<S){
            alllength -= sample_list(ran).getText().length
            val tags = sample_list(ran).getHashtagEntities().map(_.getText)
            for(tag <- tags){tag_map(tag) -= 1}
            sample_list(ran) = tw
            alllength += sample_list(ran).getText().length
            var newtags = sample_list(ran).getHashtagEntities().map(_.getText)
            for (tag <- newtags){
              if(tag_map.contains(tag)){tag_map(tag) += 1}
              else{tag_map(tag) = 1}
            }
          }
        }
      }
      println("The number of the twitter from beginning: " + N)
      println("Top 5 hot hashtags:")

      if (tag_map.size>0){
        var sort_tags = tag_map.toList.sortWith(_._2 > _._2)
        var top_tags = sort_tags.take(Math.min(5,sort_tags.size))
        for(i <- 0 until top_tags.size){
          println(top_tags(i)._1 + ":" + top_tags(i)._2)
        }
      }
      var cursize = Math.min(N,S)
      var avg_length = alllength.toDouble/cursize.toDouble
      println("The average length of the twitter is: " +  avg_length)
      println()
      println()
      println()
    }
    val stream = TwitterUtils.createStream(ssc, None)
    stream.foreachRDD(rdd => sample_func(rdd))

    ssc.start()
    ssc.awaitTermination()

  }


}

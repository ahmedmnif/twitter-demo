package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import sentimentanalysis.SentimentAnalysisUtils
import tweets.{Gettweet, Tweet}

// define a class for parsed tweets
case class Tweet(idTweet:Long,createdAt:java.sql.Date, personName:String,tweetText:String , sentiment : String)

object Main {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    System.setProperty("twitter4j.oauth.consumerKey", "AX2C0qLBowsAgNpP2lZg7A1Nn")
    System.setProperty("twitter4j.oauth.consumerSecret", "sZChyWEBCL2PAJDcbQmtvh5cFmBoMpY2mvrlEOVwhTM0qmkXAT")
    System.setProperty("twitter4j.oauth.accessToken", "996692412968980481-DBiVzZl7HzMdkYcWWvVePYkyQMqewf7")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "UyffCwEoKFVLBqpkveXggS811Zm1ujuACt3XO7LfqNXXU")
    //spark streaming context
    var ssc = new StreamingContext(sc, Seconds(5))
    // filtrage mot clé
    val filters = Seq("iphone","samsung galaxy")

    //defining stream
    val stream = TwitterUtils.createStream(ssc, None,filters)
    // filtrage by language
    val englishTweets = stream.filter(_.getLang() == "en")

    // val datatoHdfs = englishTweets.saveAsTextFiles("Englishtweets", "json")

    // mapping parsed data to the class Tweet and send data to mongo as a dataframe
    val dataToMongo = englishTweets.map(  status=>Tweet(status.getId() ,
      new java.sql.Date( status.getCreatedAt().getTime() ) ,
      status.getUser().getName(), status.getText() ,
      SentimentAnalysisUtils.detectSentiment(status.getText()).toString  )).foreachRDD({ rdd =>

      import spark.implicits._
      val mongotweet = rdd.toDF
      mongotweet.show()
      // append mode => add on the last stored data
      // MongoSpark.save(mongotweet.write.option("spark.mongodb.output.uri", "mongodb://192.168.2.8/twitterdb.twitter").mode("append"))
    })

    ssc.start()

    ssc.awaitTermination()



  }


}

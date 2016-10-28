package main.scala
  
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkTraining{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Training")
    val sc = new SparkContext(conf)
    
    case class User (id: Int, gender: String, age: Int, occupation: String, zip: String)  
    case class Rating (userId: Int, movieId: Int, rating: Int, tm: Long)
    
    val users = sc.textFile ("file:///home/cloudera/Datasets/movielens-1M/users.dat").  
      map (_.split("::")).map(row => User (row(0).toInt, row(1), row(2).toInt, row(3), row(4))).cache  
    val ratings = sc.textFile ("file:////home/cloudera/Datasets/movielens-1M/ratings.dat").  
      map (_.split("::")).map(row => Rating (row (0).toInt, row(1).toInt, row(2).toInt, row(3).toLong)).cache  
    
    users.take(5) 
    ratings.take(5)  
    
    val usersByUserId = users.map(user => (user.id, user))  
    val ratingsByUserId = ratings.map(rating => (rating.userId, rating))  
    
    val result = usersByUserId.join(ratingsByUserId).map(tuple => (tuple._1, tuple._2._1.age, tuple._2._2.rating)).
    map{case(userId, age, rating) => (age, rating)}.groupByKey.
    map{case(age, ratings) => (age, ratings.sum.toDouble/ratings.toList.size)} 
    
    result.saveAsTextFile("result");
  }
}
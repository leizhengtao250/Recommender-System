package com.spark.Reco
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.elasticsearch.spark._
object RecoModel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark SQL").setMaster("local")
    sparkConf.set("es.nodes","127.0.0.1")
    sparkConf.set("es.port","9200")
    sparkConf.set("es.index.auto.create", "false")
    sparkConf.set("es.write.operation", "index")


   //val ratings_from_es = spark.read.format("es").load("recommend/ratings").rdd
    val sc = new SparkContext(sparkConf)
    val query =
      s"""{"query":{"match_all":{}}}""".stripMargin
    val esRdd = sc.esRDD(s"recommend/ratings",query)
    print(esRdd.first())
    sc.stop()

  }

}

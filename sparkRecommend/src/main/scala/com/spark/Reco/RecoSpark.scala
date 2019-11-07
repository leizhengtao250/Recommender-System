package com.spark.Reco
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.EsSparkSQL

/**
 * ratings
 * userId,movieId,rating,timestamp
 * 1,1,4.0,964982703
 * 1,3,4.0,964981247
 * 1,6,4.0,964982224
 * 1,47,5.0,964983815
 * 1,50,5.0,964982931
 * 1,70,3.0,964982400
 * 1,101,5.0,964980868
 * 1,110,4.0,964982176
 * 1,151,5.0,964984041
 */


object RecoSpark {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark SQL").setMaster("local")
    sparkConf.set("es.nodes","127.0.0.1")
    sparkConf.set("es.port","9200")
    sparkConf.set("es.index.auto.create", "false")
    sparkConf.set("es.write.operation", "index")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val path = "/Users/hello/bigdata/data/ml-latest-small"
//    val spark = SparkSession.builder()
//      .appName("Spark SQL")
//      .master("local")
//      .config("spark.some.config.option","some-value")
//      .getOrCreate()
    val ratings = spark.read.format("csv").option("header","true").load(path+"/ratings.csv")
    import spark.implicits._
    //ratings.withColumn("timestamp",ratings("timestamp").cast(DataTypes.LongType))
    //注册自定义函数 对每个数据✖️2
    val convert:UserDefinedFunction = udf((cls:String)=>{cls.toLong*10})
    //调用udf处理ratings
    val ratings_cov=ratings.withColumn("timestamp",convert($"timestamp"))

    val raw_movies = spark.read.format("csv").option("header","true").load(path+"/movies.csv")

    //定义自定义函数，对某列数据拆分
    /**
     * movies.csv
     * movieId,title,genres
     * 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
     * 2,Jumanji (1995),Adventure|Children|Fantasy
     * 3,Grumpier Old Men (1995),Comedy|Romance
     * 4,Waiting to Exhale (1995),Comedy|Drama|Romance
     *
     */
    val genresSplit:UserDefinedFunction = udf((cls:String)=>{cls.toLowerCase.split("\\|")})
    val raw_moive_temp=raw_movies.withColumn("genres",genresSplit($"genres"))

    //提取title年份到year
    val movies=raw_moive_temp.withColumn("release_date",regexp_extract($"title","\\d{4}",0))

    /**
     * links.csv
     * movieId,imdbId,tmdbId
     * 1,0114709,862
     * 2,0113497,8844
     * 3,0113228,15602
     * 4,0114885,31357
     */

    val link_data = spark.read.format("csv").option("header","true").load(path+"/links.csv")
    /**
     * 处理过的raw_movie和link表join
     */

    val movie_data=movies.join(link_data,raw_movies("movieId")===link_data("movieId"))
    movie_data.printSchema()
    ratings_cov.printSchema()
    movie_data.show(20)

    /**
     * 将处理过的数据写入到elasticsearch 中
     */
   //EsSparkSQL.saveToEs(ratings_cov,"recommend/ratings")
//    print("Number of ratings:"+ratings.count()+"\n")
   // EsSparkSQL  .saveToEs(movie_data,"recommend/movies")

    /**
     * |-- movieId: string (nullable = true)
     * |-- title: string (nullable = true)
     * |-- genres: array (nullable = true)
     * |    |-- element: string (containsNull = true)
     * |-- year: string (nullable = true)
     * |-- movieId: string (nullable = true)
     * |-- imdbId: string (nullable = true)
     * |-- tmdbId: string (nullable = true)
     */





    //    val csvopt = spark.read.format("csv").option("header","true").load("/Users/hello/bigdata/data/book_labelv1.csv")
//    val getCN  : UserDefinedFunction =udf((cls:String)=>{
//      //去除字母和数字和空格,只提取中文
//      new Regex("[\\u4e00-\\u9fa5]").findAllIn(cls).mkString("").toString()
//
//    })
//
//
//    /**
//     * 判断大小
//     */
//    val judge :UserDefinedFunction = udf((cls:String)=>{
//      if(cls.length>3){
//        val a = cls.substring(0,3)
//        return a
//      }else{
//        return cls
//      }
//    })


//    csvCN.withColumn("label"+1,judge($"label"+1)).show(10)








    //关闭资源
    spark.stop()





  }

}

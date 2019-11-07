package com.spark.Reco

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object ProcessChen {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local")
    //val sc = new SparkContext(sparkConf)

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val path = "/Users/hello/bigdata/data/finaldouban/"
    val bid_bname = spark.read.format("csv").option("header","true").load(path+"/quchongbid_bname.csv")
    val user_rate= spark.read.format("csv").option("header","true").load(path+"/user_rate.csv")
    val data = user_rate.join(bid_bname,bid_bname("book_name") === user_rate("user_book_name"))
    data.coalesce(1).write.mode("Append").csv(path+"user_rate_book_id")






//    val bid_bname = spark.read.format("csv").option("header","true").load(path+"/bookid_bookname.csv")
//    bid_bname.createOrReplaceTempView("temp")
//    val sql = "select distinct book_id,book_name from temp"
//    val out = spark.sql(sql).coalesce(1)
//    out.write.mode("Append").csv(path+"/quchongbook")



    //user_rate
    //xiechangliu,流血的仕途：4
    //val user_rate = spark.read.format("csv").option("header","false").load(path+"/user_rate.csv/user_rate.csv")

//        val FileRDD = spark.sparkContext.textFile(path+"/user_rate.csv/tmp/user_rate.txt")
//          .map(x=>x.split((",")))
//          .map(x=> {
//            if (x.length > 2) {
//              Row(x(0), x(1), x(2))
//            }else if(x.length>1){
//              Row(x(0),x(1),"")
//            }else{
//              Row(x(0),"","")
//            }
//
//          })
//        val schema = StructType{
//          Seq(
//            StructField("user_id",StringType,true)
//            ,StructField("book_id",StringType,true)
//            ,StructField("rate",StringType,true)
//
//          )
//
//        }
//        val DF = spark.createDataFrame(FileRDD,schema)
//        DF.write.csv(path+"/user_rate.csv/tmp/user_rate.csv")


//    val user_rate_rdd = user_rate.map(st =>{
//      val s = st.split(",")
//      val user_id = s(0)
//      val s2 = s(1).split("：")
//      val book_name = s2(0)
//      if  (s2.length>1){
//        val rate = s2(1)
//        user_id+","+book_name+","+rate
//      }else{
//        user_id+","+book_name+","
//      }
//
//    })

//    user_rate_rdd.repartition(1).saveAsTextFile(path+"/user_rate.csv/tmp")
//        textrdd.repartition(1).saveAsTextFile(path+"/user")
//








    //bookid_bookname
    //10598170,我不要你死于一事无成












//    val FileRDD = spark.sparkContext.textFile(path+"user_rate.txt")
//      .map(x=>x.split((",")))
//      .map(x=>Row(x(0),x(1)))
//    val schema = StructType{
//      Seq(
//        StructField("user_id",StringType,true)
//        ,StructField("user_rate",StringType,true)
//        //,StructField("user_link",StringType,true)
//
//      )
//
//    }
//    val DF = spark.createDataFrame(FileRDD,schema)
//    DF.write.csv(path+"user_rate.csv")




//    val text = sc.textFile(path+"/用户.txt")
//    val textrdd=text.map(st =>{
//      val s =  st.split(":")
//      val s1 = s(1).split(",")(0).split("'")(1)//xiechangliu
//      val s2 = s(2).split(",")(0).split("'")(1)//卢卡斯
//      val s3=s(3).split("'")(1)
//      val s4=s(4).split("'")(0)
//      val s5=s3+":"+s4        //https://www.douban.com/people/xiechangliu/
//      s1+","+s2+","+s5
//    }
//    )
//
//    textrdd.repartition(1).saveAsTextFile(path+"/user")

//    val text = sc.textFile(path+"/书.txt")
//    val textrdd = text.map(st=>{
//      val s = st.split(":")
//      val s1=s(1).split(",")(0).split("'")(1)
//      val s2=new Regex("(\\[[^\\]]*\\])").findAllIn(st).mkString("").toString()
//      val s3 =s2.replace("'","").replaceAll("\\[","").replaceAll("]","")
//      s1+"|"+s3
//
//    })

//
//    val text = sc.textFile(path+"/book_label/part-00000")
//    val textrdd = text.flatMap(st=>{
//      val s = st.split(s"\\|")
//      val s1=s(0)
//
//      if (s.length>1){
//      val s2 =s(1).split(",")
//      for(i<- 0 until s2.length) yield{
//        s1+","+s2(i)
//      }}else{
//        ""
//      }
//    })
//
//    textrdd.repartition(1).saveAsTextFile(path+"/book_label_tmp")

//
//     val text = sc.textFile(path+"/user_read_tmp/part-00000")
//         val textrdd = text.map(st=>{
//         val s = st.split(":")
//         val s1=s(1).split(",")(0).split("'")(1)
//         val s2=new Regex("(\\[[^\\]]*\\])").findAllIn(st).mkString("").toString()
//         val s3 =s2.replace("'","").replaceAll("\\[","").replaceAll("]","")
//         s1+"|"+s3
//       })

//        val textrdd = text.flatMap(st=>{
//          val s = st.split(s"\\|")
//          val s1=s(0)
//
//          if (s.length>1){
//          val s2 =s(1).split(",")
//          for(i<- 0 until s2.length) yield{
//            s1+","+s2(i)
//          }}else{
//            ""
//          }
//        })
//    textrdd.repartition(1).saveAsTextFile(path+"/user_read")

//    val text = sc.textFile(path+"/user_rate_tmp/part-00000")
//
//            val textrdd = text.flatMap(st=>{
//              val s = st.split(s"\\|")
//              val s1=s(0)
//
//              if (s.length>1){
//              val s2 =s(1).split(",")
//              for(i<- 0 until s2.length) yield{
//                s1+","+s2(i)
//              }}else{
//                ""
//              }
//            })
//    textrdd.repartition(1).saveAsTextFile(path+"/user_rate")

//


  spark.stop()





    }


  }



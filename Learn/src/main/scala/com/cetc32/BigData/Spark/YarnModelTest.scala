package com.cetc32.BigData.Spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object YarnModelTest extends App{
  val conf=new SparkConf().setAppName("TestYarn").setMaster("local[*]")
  val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  //连接数据库
  spark.sql("use test")
  spark.sql("insert into student values (4,'andy')")
  val databases=spark.sql("select * from student")
  databases.show()
}

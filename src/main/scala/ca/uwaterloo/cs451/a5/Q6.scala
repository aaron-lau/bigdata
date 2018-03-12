package ca.uwaterloo.cs451.a5
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession

object Q6 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val shipdate = args.date()

    if (args.text()){
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => (line, line.split("""\|""")(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map( line => {
          var columns = line._1.split("""\|""")
          val returnflag = columns(8)
          val linestatus = columns(9)
          val quantity = columns(4).toLong
          val discount = columns(6).toDouble
          val tax = columns(7).toDouble
          val extendedprice = columns(5).toDouble
          val discprice = extendedprice * (1 - discount)
          val sumcharge = extendedprice * (1 - discount) * (1 + tax)
          ((returnflag,linestatus),(quantity,extendedprice,discprice,sumcharge,discount,1))
        })
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey(true)
        .map(pair=>{
           val sum_qty = pair._2._1
           val sum_base_price = pair._2._2
           val sum_disc_price = pair._2._3
           val sum_charge = pair._2._4
           val count_order = pair._2._6
           val avg_qty = sum_qty.toDouble / count_order
           val avg_price = sum_base_price / count_order
           val avg_disc = pair._2._5 / count_order
           (pair._1._1, pair._1._2, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
         })
      lineitemRDD.collect().foreach(println)
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemRDDFiltered = lineitemRDD
        .map(line => (line, line.getString(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map( line => {
          var columns = line._1
          val returnflag = columns.getString(8)
          val linestatus = columns.getString(9)
          val quantity = columns.getDouble(4).toLong
          val discount = columns.getDouble(6)
          val tax = columns.getDouble(7)
          val extendedprice = columns.getDouble(5)
          val discprice = extendedprice * (1 - discount)
          val sumcharge = extendedprice * (1 - discount) * (1 + tax)
          ((returnflag,linestatus),(quantity,extendedprice,discprice,sumcharge,discount,1))
        })
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey(true)
        .map(pair=>{
           val sum_qty = pair._2._1
           val sum_base_price = pair._2._2
           val sum_disc_price = pair._2._3
           val sum_charge = pair._2._4
           val count_order = pair._2._6
           val avg_qty = sum_qty.toDouble / count_order
           val avg_price = sum_base_price / count_order
           val avg_disc = pair._2._5 / count_order
           (pair._1._1, pair._1._2, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
         })
       lineitemRDDFiltered.collect().foreach(println)
    }
  }
}

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

// class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
//   // mainOptions = Seq(input, date, )
//   val input = opt[String](descr = "input path", required = true)
//   val date = opt[String](descr = "l_shipdate", required = true)
//   val text = opt[Boolean](descr = "parquet", required = false)
//   val parquet = opt[Boolean](descr = "parquet", required = false)
//   verify()
// }
//
// class MyPartitioner(numOfPar: Int) extends Partitioner {
//   def numPartitions: Int = numOfPar
//   def getPartition(key: Any): Int = {
//     val k = key.asInstanceOf[(String, String)]
//     ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
//   }
// }

object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val shipdate = args.date()
    // val lineitemRDD =

    if (args.text()){
      val lineitemRDD = sc.textFile(args.input()+"/lineitem.tbl")
        .map(line => (line.split("""\|""")(0), line.split("""\|""")(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)

      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        .map(order => (order.split("""\|""")(0), order.split("""\|""")(6)))

      val results = ordersRDD.cogroup(lineitemRDD)
        // filter where Key appears in both rdds
        .filter(_._2._2.size!=0)
        // map the results from [(orderkey, (clerk, shipdate))] => [(orderkey,clerk)]
        .map(pair=>(pair._1.toLong,pair._2._1.head))
        // order by orderkey
        .sortByKey(true)
        // limit 20
        .take(20)
        // reorder to [(clerk,orderkey)]
        .map(pair=>(pair._2,pair._1))

      results.foreach(println)
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd

      val lineitemRDDFiltered = lineitemRDD
        .map(line => (line.getInt(0).toString, line.getString(10)))
        .filter(_._2.substring(0,shipdate.length())==shipdate)

      val ordersRDDFiltered = ordersRDD
        .map(order => (order.getInt(0).toString, order.getString(6)))

      val results = ordersRDDFiltered.cogroup(lineitemRDDFiltered)
        // filter where Key appears in both rdds
        .filter(_._2._2.size!=0)
        // map the results from [(orderkey, (clerk, shipdate))] => [(orderkey,clerk)]
        .map(pair=>(pair._1.toLong,pair._2._1.head))
        // order by orderkey
        .sortByKey(true)
        // limit 20
        .take(20)
        // reorder to [(clerk,orderkey)]
        .map(pair=>(pair._2,pair._1))

      results.foreach(println)
    }
  }
}

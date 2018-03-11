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

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  // mainOptions = Seq(input, date, )
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate", required = false)
  val text = opt[Boolean](descr = "parquet", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}

class MyPartitioner(numOfPar: Int) extends Partitioner {
  def numPartitions: Int = numOfPar
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, String)]
    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
  }
}

object Q1 extends Tokenizer {
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
      val counts = lineitemRDD
              .map(line => line.split("""\|""")(10))
              .filter(_.substring(0,shipdate.length())==shipdate)
              .map(date=>("count",1))
              .reduceByKey(_ + _)

      println("ANSWER="+counts.lookup("count")(0))
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      // lineitemRDD.take(10).foreach(println)
      val counts = lineitemRDD
              .map(line => line.getString(10))
              // .filter(_.equals(shipdate)
              .filter(_.substring(0,shipdate.length())==shipdate)
              .map(date=>("count",1))
              .reduceByKey(_ + _)
      //
      println("ANSWER="+counts.lookup("count")(0))
    }
  }
}

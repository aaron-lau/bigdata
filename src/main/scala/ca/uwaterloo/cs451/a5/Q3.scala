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

object Q3 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val shipdate = args.date()
    // val lineitemRDD =

    if (args.text()){
      val part = sc.textFile(args.input() + "/part.tbl")
        .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))

      val partBroadcast = sc.broadcast(part.collectAsMap())

      val supplier = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => (line.split("""\|""")(0), line.split("""\|""")(1)))

      val supplierBroadcast = sc.broadcast(supplier.collectAsMap())

      val results = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => (line, line.split("""\|""")(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map(pair => {
          val orderkey = pair._1.split("""\|""")(0)
          val partkey = pair._1.split("""\|""")(1)
          val suppkey = pair._1.split("""\|""")(2)
          val partTable = partBroadcast.value.get(partkey)
          val supplierTable = supplierBroadcast.value.get(suppkey)
          (orderkey.toLong, (partTable,supplierTable))
        })
        .sortByKey(true)
        .take(20)

      results.foreach{ pair=>
        pair match{
          case (orderkey,(Some(partkey),Some(suppkey))) => println("("+orderkey+","+partkey+","+suppkey+")")
          case _ =>  println()
        }
      }
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet(args.input()+"/part")
      val partRDD = partDF.rdd
      val partRDDFilterd = partRDD
        .map(part => (part.getInt(0).toString, part.getString(1)))
      val partBroadcast = sc.broadcast(partRDDFilterd.collectAsMap())

      val supplierDF = sparkSession.read.parquet(args.input()+"/supplier")
      val supplierRDD = supplierDF.rdd
      val supplierRDDFilterd = supplierRDD
        .map(supplier => (supplier.getInt(0).toString, supplier.getString(1)))
      val supplierBroadcast = sc.broadcast(supplierRDDFilterd.collectAsMap())

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val results = lineitemDF.rdd
        .map(line => (line, line.getString(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map(pair => {
          val orderkey = pair._1.getInt(0).toString
          val partkey = pair._1.getInt(1).toString
          val suppkey = pair._1.getInt(2).toString
          val partTable = partBroadcast.value.get(partkey)
          val supplierTable = supplierBroadcast.value.get(suppkey)
          (orderkey.toLong, (partTable,supplierTable))
        })
        .sortByKey(true)
        .take(20)

      results.foreach{ pair=>
        pair match{
          case (orderkey,(Some(partkey),Some(suppkey))) => println("("+orderkey+","+partkey+","+suppkey+")")
          case _ =>  println()
        }
      }

    }
  }
}

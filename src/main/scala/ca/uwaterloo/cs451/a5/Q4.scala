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

object Q4 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val shipdate = args.date()
    // val lineitemRDD =

    if (args.text()){
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        .map(customer => (customer.split("""\|""")(0), customer.split("""\|""")(3)))
      val customerBroadcast = sc.broadcast(customerRDD.collectAsMap())

      val nationRDD = sc.textFile(args.input() + "/nation.tbl")
        .map(nation => (nation.split("""\|""")(0), nation.split("""\|""")(1)))
      val nationBroadcast = sc.broadcast(nationRDD.collectAsMap())

      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => (line.split("""\|""")(0), line.split("""\|""")(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map(pair => (pair._1, 1))
        .reduceByKey(_ + _)

      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        .map(order => (order.split("""\|""")(0), order.split("""\|""")(1)))

      val results = lineitemRDD.cogroup(ordersRDD)
        // filter where Key appears in both rdds)
        .filter(_._2._1.size != 0)
        // map the results from [(orderkey, (1, custkey))] => [(Some(nationkey), 1)]
        .map(pair => {
          val customerTable = customerBroadcast.value
          (customerTable.get(pair._2._2.head), pair._2._1.head)
        })
        .reduceByKey(_ + _)
        // map the results from [(Some(nationkey), 1)] => [(nationkey, 1)]
        .map( pair =>
          pair match {
            case (Some(nationkey), count) => (nationkey.toInt, count)
          }
        )
        // map the results from [(nationkey, 1)] => [(nationkey, (nationname,1))]
        .map(pair =>
          {
            val nationTable = nationBroadcast.value
            nationTable.get(pair._1.toString()) match {
              case (Some(nationname)) => (pair._1, (nationname, pair._2))
            }
          })
        .sortByKey(true)

      results.collect().foreach { pair =>
        println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")")
      }
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input()+"/customer")
      val customerRDD = customerDF.rdd
      val customerRDDFilterd = customerRDD
        .map(customer => (customer.getInt(0).toString, customer.getInt(3).toString))
      val customerBroadcast = sc.broadcast(customerRDDFilterd.collectAsMap())

      val nationDF = sparkSession.read.parquet(args.input()+"/nation")
      val nationRDD = nationDF.rdd
      val nationRDDFilterd = nationRDD
        .map(nation => (nation.getInt(0).toString, nation.getString(1)))
      val nationBroadcast = sc.broadcast(nationRDDFilterd.collectAsMap())

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemRDDFiltered = lineitemRDD
        .map(line => (line.getInt(0).toString, line.getString(10)))
        .filter(_._2.substring(0, shipdate.length()) == shipdate)
        .map(pair => (pair._1, 1))
        .reduceByKey(_ + _)

      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val ordersRDDFilteredRDD = ordersRDD
        .map(order => (order.getInt(0).toString, order.getInt(1).toString))

      val results = lineitemRDDFiltered.cogroup(ordersRDDFilteredRDD)
        // filter where Key appears in both rdds)
        .filter(_._2._1.size != 0)
        // map the results from [(orderkey, (1, custkey))] => [(Some(nationkey), 1)]
        .map(pair => {
          val customerTable = customerBroadcast.value
          (customerTable.get(pair._2._2.head), pair._2._1.head)
        })
        .reduceByKey(_ + _)
        // map the results from [(Some(nationkey), 1)] => [(nationkey, 1)]
        .map( pair =>
          pair match {
            case (Some(nationkey), count) => (nationkey.toInt, count)
          }
        )
        // map the results from [(nationkey, 1)] => [(nationkey, (nationname,1))]
        .map(pair =>
          {
            val nationTable = nationBroadcast.value
            nationTable.get(pair._1.toString()) match {
              case (Some(nationname)) => (pair._1, (nationname, pair._2))
            }
          })
        .sortByKey(true)

      results.collect().foreach { pair =>
        println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")")
      }

    }
  }
}

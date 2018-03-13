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

object Q5 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()){
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        .map(customer => (customer.split("""\|""")(0), customer.split("""\|""")(3)))
      val customerBroadcast = sc.broadcast(customerRDD.collectAsMap())

      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        .map(order => (order.split("""\|""")(0), order.split("""\|""")(1)))
        // map the results from [(orderkey, custkey)] => [(orderkey, nationkey)]
        .map( pair => {
          val customerTable = customerBroadcast.value
          customerTable.get(pair._2) match {
            case (Some(nationkey)) => (pair._1, nationkey.toInt)
          }
        })
        // 3 == Canada
        // 24 == United States
        .filter(pair => (pair._2 == 3 | pair._2 ==24))

      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        // substring to only get the years and months
        .map(line => (line.split("""\|""")(0), line.split("""\|""")(10).substring(0,7)))
        // cogroup [(orderkey, shipdate)] => [(orderkey, (shipdate, nationkey))]
        .cogroup(ordersRDD)
        .filter(_._2._2.size != 0)
        .flatMap(pair => {
          val shipdateList = pair._2._1.toList
          val nationkey = pair._2._2.head
          shipdateList.map(shipdate => ((shipdate, nationkey), 1)).toList
        })
        .reduceByKey(_ + _)
        .sortByKey(true)
        .map(pair=> (pair._1._2, (pair._1._1, pair._2 )))
        .sortByKey(true)

      lineitemRDD.collect().foreach{pair =>
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

      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val ordersRDDFilteredRDD = ordersRDD
        .map(order => (order.getInt(0).toString, order.getInt(1).toString))
        // map the results from [(orderkey, custkey)] => [(orderkey, nationkey)]
        .map( pair => {
          val customerTable = customerBroadcast.value
          customerTable.get(pair._2) match {
            case (Some(nationkey)) => (pair._1, nationkey.toInt)
          }
        })
        // 3 == Canada
        // 24 == United States
        .filter(pair => (pair._2 == 3 | pair._2 == 24))

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemRDDFiltered = lineitemRDD
        .map(line => (line.getInt(0).toString, line.getString(10).substring(0,7)))
        .cogroup(ordersRDDFilteredRDD)
        .filter(_._2._2.size != 0)
        .flatMap(pair => {
          val shipdateList = pair._2._1.toList
          val nationkey = pair._2._2.head
          shipdateList.map(shipdate => ((shipdate, nationkey), 1)).toList
        })
        .reduceByKey(_ + _)
        .sortByKey(true)
        .map(pair=> (pair._1._2, (pair._1._1, pair._2 )))
        .sortByKey(true)

      lineitemRDDFiltered.collect().foreach { pair =>
        println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")")
      }
    }
  }
}

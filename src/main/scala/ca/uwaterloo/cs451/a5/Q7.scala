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

object Q7 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val shipdate = args.date()

    // select
    //   c_name,
    //   l_orderkey,
    //   sum(l_extendedprice*(1-l_discount)) as revenue,
    //   o_orderdate,
    //   o_shippriority
    // from customer, orders, lineitem
    // where
    //   c_custkey = o_custkey and
    //   l_orderkey = o_orderkey and
    //   o_orderdate < "YYYY-MM-DD" and
    //   l_shipdate > "YYYY-MM-DD"
    // group by
    //   c_name,
    //   l_orderkey,
    //   o_orderdate,
    //   o_shippriority
    // order by
    //   revenue desc
    // limit 10;

    if (args.text()){
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        .map(customer => (customer.split("""\|""")(0), customer.split("""\|""")(1)))
      // custKey, name
      val customerBroadcast = sc.broadcast(customerRDD.collectAsMap())

      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => (line, line.split("""\|""")(10)))
        .filter(_._2 > shipdate)
        .map(pair => {
          val orderkey = pair._1.split("""\|""")(0).toLong
          val extendedprice = pair._1.split("""\|""")(5).toDouble
          val discount = pair._1.split("""\|""")(6).toDouble
          val revenue = extendedprice * (1 - discount)
          (orderkey, revenue)
        })
        .reduceByKey(_ + _)

      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        .map(line => (line, line.split("""\|""")(4)))
        .filter(_._2 < shipdate)
        .map(pair => {
          val orderkey = pair._1.split("""\|""")(0).toLong
          val customerTable = customerBroadcast.value
          val someName = customerTable.get(pair._1.split("""\|""")(1))
          val name = {
            someName match {
              case Some(someName) => someName
            }
          }
          val orderdate = pair._2
          val shippriority = pair._1.split("""\|""")(7)
          (orderkey, (name, orderdate, shippriority))
        })

      val results = lineitemRDD.cogroup(ordersRDD)
        // [(orderkey, ((revenue), (name, orderdate, shippriority)))]
        .filter { pair => (pair._2._1.size != 0 & pair._2._2.size != 0) }
        .map(pair => {
          val name = pair._2._2.head._1
          val orderkey = pair._1
          val revenue = pair._2._1.head
          val orderdate = pair._2._2.head._2
          val shippriority = pair._2._2.head._3
          (revenue, (name, orderkey, orderdate, shippriority))

        })
        // sort by revenue desc
        .sortByKey(false)
        // reorder (revenue, (name, orderkey, orderdate, shippriority))
        // to (name, orderkey, revenue, orderdate, shippriority)
        .map(pair => (pair._2._1, pair._2._2, pair._1, pair._2._3, pair._2._4))

      // limit 10
      results.collect().take(10).foreach(println)
    }
    else if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input()+"/customer")
      val customerRDD = customerDF.rdd
      val customerRDDFilterd = customerRDD
        .map(customer => (customer.getInt(0).toString, customer.getString(1)))
      val customerBroadcast = sc.broadcast(customerRDDFilterd.collectAsMap())

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitemRDDFiltered = lineitemRDD
        .map(line => (line, line.getString(10)))
        .filter(_._2 > shipdate)
        .map( line => {
          var columns = line._1
          val orderkey = columns.getInt(0)
          val extendedprice = columns.getDouble(5)
          val discount = columns.getDouble(6)
          val revenue = extendedprice * (1 - discount)
          (orderkey, revenue)
        })
        .reduceByKey(_ + _)

      val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
      val ordersRDD = ordersDF.rdd
      val ordersRDDFilteredRDD = ordersRDD
        .map(order => (order, order.getString(4)))
        .filter(_._2 < shipdate)
        .map(pair => {
          val orderkey = pair._1.getInt(0)
          val customerTable = customerBroadcast.value
          val someName = customerTable.get(pair._1.getInt(1).toString)
          val name = {
            someName match {
              case Some(someName) => someName
            }
          }
          val orderdate = pair._2
          val shippriority = pair._1.getInt(7)
          (orderkey, (name, orderdate, shippriority))
        })

      val results = lineitemRDDFiltered.cogroup(ordersRDDFilteredRDD)
        // [(orderkey, ((revenue), (name, orderdate, shippriority)))]
        .filter { pair => (pair._2._1.size != 0 & pair._2._2.size != 0) }
        .map(pair => {
          val name = pair._2._2.head._1
          val orderkey = pair._1
          val revenue = pair._2._1.head
          val orderdate = pair._2._2.head._2
          val shippriority = pair._2._2.head._3
          (revenue, (name, orderkey, orderdate, shippriority))
        })
        // sort by revenue desc
        .sortByKey(false)
        // reorder (revenue, (name, orderkey, orderdate, shippriority))
        // to (name, orderkey, revenue, orderdate, shippriority)
        .map(pair => (pair._2._1, pair._2._2, pair._1, pair._2._3, pair._2._4))

      // limit 10
      results.collect().take(10).foreach(println)

    }
  }
}

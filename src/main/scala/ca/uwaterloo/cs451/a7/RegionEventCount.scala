/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    // val goldmanCoords = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
    // val citigroupCoords = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]]

    val g_minx = -74.0144185
    val g_maxx = -74.013777
    val g_miny = 40.7138745
    val g_maxy = 40.7152275

    val c_minx = -74.012083
    val c_maxx = -74.009867
    val c_miny = 40.720053
    val c_maxy = 40.7217236

    // yellow
    // type,VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RatecodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
    // 11, 12
    // green
    // type,VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,improvement_surcharge,Total_amount,Payment_type,Trip_type
    // 9, 10

    val wc = stream.map(_.split(","))
      .map( line => {
        val taxitype = line(0)
        var dropoff_longitude = 0.0
        var dropoff_latitude = 0.0
        var company = ""
        if (taxitype == "green"){
          dropoff_longitude = line(8).toDouble
          dropoff_latitude = line(9).toDouble
        }
        else if (taxitype == "yellow") {
          dropoff_longitude = line(10).toDouble
          dropoff_latitude = line(11).toDouble
        }
        if (dropoff_longitude > g_minx && dropoff_longitude < g_maxx && dropoff_latitude > g_miny && dropoff_latitude < g_maxy){
          company = "goldman"
        }
        else if (dropoff_longitude > c_minx && dropoff_longitude < c_maxx && dropoff_latitude > c_miny && dropoff_latitude < c_maxy){
          company = "citigroup"
        }
        (company, 1)
      })
        // case tuple if (tuple(0) == "yellow" && tuple(10).toDouble > g_minx && tuple(10).toDouble < g_maxx && tuple(11).toDouble > g_miny && tuple(11).toDouble < g_maxy) => ("goldman", 1)
        // case tuple if (tuple(0) == "green" && tuple(8).toDouble > g_minx && tuple(8).toDouble < g_maxx && tuple(9).toDouble > g_miny && tuple(9).toDouble < g_maxy) => ("goldman", 1)
        // case tuple if (tuple(0) == "yellow" && tuple(10).toDouble > c_minx && tuple(10).toDouble < c_maxx && tuple(11).toDouble > c_miny && tuple(11).toDouble < c_maxy) => ("citigroup", 1)
        // case tuple if (tuple(0) == "green" && tuple(8).toDouble > c_minx && tuple(8).toDouble < c_maxx && tuple(9).toDouble > c_miny && tuple(9).toDouble < c_maxy) => ("citigroup", 1)
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

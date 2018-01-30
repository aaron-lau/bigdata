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

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class myPartitionerPMIStripes(partitionsNum: Int) extends Partitioner {
 override def numPartitions: Int = partitionsNum
 override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    return ( (k.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt
  }
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    //Counts Bigrams
    val bigGramCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList.take(40) else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)

    //Counts words
    val counts = textFile
        .flatMap(line => tokenize(line).take(40))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .saveAsTextFile(args.output())

    //Sum all count of tweets
    val totalLineCount = textFile.count()

  }
}

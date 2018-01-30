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

class myPartitionerPMIPairs(partitionsNum: Int) extends Partitioner {
 override def numPartitions: Int = partitionsNum
 override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    return ( (k.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt
  }
}

class ConfPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = true)
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")
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
      .sortByKey()

    //Counts words
    val counts = textFile
        .flatMap(line => tokenize(line).take(40))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortByKey()

    val totalLineCount = textFile.count()

    //Sum all count of tweets
    // val PMI = bigGramCounts.flatMap( bigram => {
    //   val firstWord = bigram._1.split(" ")(0)
    //   val secondWord = bigram._1.split(" ")(1)
    //   val pxy = bigram._2.toFloat / totalLineCount.toFloat
    //   val firstWordList = counts.lookup(firstWord)
    //   val secondWordList = counts.lookup(secondWord)
    //   val totalFirstWord = 0
    //   val totalSecondWord = 0
    //   firstWordList.foreach(totalFirstWord = + totalFirstWord)
    //   secondWordList.foreach(totalSecondWord = + totalSecondWord)
    //   val px = totalFirstWord.toFloat / totalLineCount.toFloat
    //   val py = totalSecondWord.toFloat / totalLineCount.toFloat
    //   val PMI: Double = scala.math.log(pxy / (px * py))
    //   List(bigram._1, PMI)
    // })
    // .map(pmiAnswer => (pmiAnswer._1, pmiAnswer._2))
    // .reduceByKey(_ + _)
    // .sortByKey()

    counts.saveAsTextFile(args.output())


  }
}

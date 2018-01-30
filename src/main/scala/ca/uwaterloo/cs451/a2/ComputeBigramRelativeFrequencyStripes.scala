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

class myPartitionerStripes(partitionsNum: Int) extends Partitioner {
 override def numPartitions: Int = partitionsNum
 override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    return ( (k.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt
  }
}

// Will import class Conf from ComputeBigramRelativeFrequencyPairs
object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens1 = tokenize(line)
        if (tokens1.length > 1) tokens1.sliding(2).toList
        // Create a mapping of that groups togethers pairs
        // where the second word is the key and 1 is the
          .map(bigram => {
            val mapInit = scala.collection.immutable.Map[String, Int]()
            val mapping = mapInit + (bigram(1) -> 1)
            (bigram(0), mapping)
          })
        else List()
      })
      // Do an element-wise sum of associative arrays
      // else the base case is add 0
      .reduceByKey((x,y) => x ++ y.map{ case (k,v) => k -> (x.getOrElse(k,0))})
      .sortByKey()
      .partitionBy(new myPartitionerStripes(args.reducers()))
      .mapPartitions(occurrenceMapping => {
        occurrenceMapping.map(coccurrenceMapping=> {
          // total count of first word
          var totalCount = 0.0f
          // make a mutable object so we can calculate total occurrence
          var mappingMutable = scala.collection.mutable.Map(coccurrenceMapping._2.toSeq: _*)
          // iterate the mutable object to add all the mappings to totalCount
          mappingMutable.foreach {case(key,value) => {totalCount = totalCount + value.toFloat}}
          // calculate the relativeFrequency for each map in the mutable object
          var relativeFrequency = mappingMutable.map {case(key,value) => (key, value.toFloat/totalCount)}
          (coccurrenceMapping._1, relativeFrequency)
        })
      }
    )
    counts.saveAsTextFile(args.output())
  }
}

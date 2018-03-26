package ca.uwaterloo.cs451.a6
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
import scala.math._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  // mainOptions = Seq(input, date, )
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "to shuffle?", required = false, default = Option(false))
  verify()
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val shuffle = args.shuffle()

    val trained = if (!shuffle) {sc.textFile(args.input()).map(line => {
      // Parse input
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      var isSpam = 0
      if (instanceArray(1).equals("spam")) {
        isSpam = 1
      }
      val features = instanceArray.slice(2, instanceArray.length).map { featureIndex => featureIndex.toInt }
      (0, (docid, isSpam, features))
    }).coalesce(1).groupByKey(1).persist()} else {
      sc.textFile(args.input()).map(line => {
        // Parse input
        val instanceArray = line.split(" ")
        val docid = instanceArray(0)
        var isSpam = 0
        if (instanceArray(1).equals("spam")) {
          isSpam = 1
        }
        val features = instanceArray.slice(2, instanceArray.length).map { featureIndex => featureIndex.toInt }
        (0, (docid, isSpam, features))
      }).map(pair => {
          val r = scala.util.Random
          (r.nextInt(), pair._2)
        })
        .sortByKey()
        .map(pair => (0, pair._2))
        .groupByKey(1).persist()
    }

    // w is the weight vector (make sure the variable is within scope)
    var w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    val new_w = trained.mapPartitions(indexIterator => {
          // indexIterator.foreach(instanceIterable => {
          // val tuple = instanceIterable._2
          val instanceIterable = indexIterator.next._2
          instanceIterable.foreach(tuple => {
          // For each instance...
          // label
          val isSpam = tuple._2
          // feature vector of the training instance
          val features = tuple._3
          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w = w updated (f, w(f) + (isSpam - prob) * delta)
            } else {
              w = w updated (f, (isSpam - prob) * delta)
            }
          })
        })
        w.toIterator
      })
    new_w.saveAsTextFile(args.model());
  }
}

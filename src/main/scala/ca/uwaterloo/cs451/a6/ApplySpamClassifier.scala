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

class Conf2(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  // mainOptions = Seq(input, date, )
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ApplySpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelFile = sc.textFile(args.model())
    val weight = modelFile.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastWeight = sc.broadcast(weight)
    val w = broadcastWeight.value

    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    val testLabel = sc.textFile(args.input()).map(line=>{
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      val isSpamlabel = instanceArray(1)
      val features = instanceArray.slice(2, instanceArray.length).map{ featureIndex => featureIndex.toInt }
      val spamScore = spamminess(features)

      var isSpamJudge = "spam"
      if (!(spamScore > 0)) {
        isSpamJudge = "ham"
      }
      (docid, isSpamlabel, spamScore, isSpamJudge)
    })
    .saveAsTextFile(args.output())
  }
}

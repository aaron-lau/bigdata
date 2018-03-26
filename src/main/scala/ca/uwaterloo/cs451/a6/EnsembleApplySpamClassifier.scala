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

class Conf3(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  // mainOptions = Seq(input, date, )
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "average/vote?", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelFile1 = sc.textFile(args.model()+"/part-00000")
    val weight1 = modelFile1.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastWeightX = sc.broadcast(weight1)

    val modelFile2 = sc.textFile(args.model()+"/part-00001")
    val weight2 = modelFile2.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastWeightY = sc.broadcast(weight2)

    val modelFile3 = sc.textFile(args.model()+"/part-00002")
    val weight3 = modelFile3.map(line => (line.split("[,()]")(1).toInt, line.split("[,()]")(2).toDouble)).collectAsMap
    val broadcastWeightB = sc.broadcast(weight3)

    val wX = broadcastWeightX.value
    val wY = broadcastWeightY.value
    val wB = broadcastWeightB.value

    def spamminess(features: Array[Int]): Array[Double] = {
      var scoreX = 0d
      var scoreY = 0d
      var scoreB = 0d
      features.foreach(f => {
        if (wX.contains(f)) scoreX += wX(f)
        if (wY.contains(f)) scoreY += wY(f)
        if (wB.contains(f)) scoreB += wB(f)
      })
      Array(scoreX, scoreY, scoreB)
    }

    var method = ""
    if (args.method().equals("vote")) {
      method = "vote"
    } else if (args.method().equals("average")) {
      method = "average"
    }

    val testLabel = sc.textFile(args.input()).map(line=>{
      val instanceArray = line.split(" ")
      val docid = instanceArray(0)
      val isSpamlabel = instanceArray(1)
      val features = instanceArray.slice(2, instanceArray.length).map{ featureIndex => featureIndex.toInt }
      val spamScoreArray = spamminess(features)
      var spamScore=0d

      if (method.equals("average")){
        spamScore=((spamScoreArray(0)+spamScoreArray(1)+spamScoreArray(2))/spamScoreArray.size)
      } else if (method.equals("vote")){
        val sigArray= for {score<-spamScoreArray} yield Math.signum(score).toInt
        spamScore = sigArray(0)+sigArray(1)+sigArray(2)
      }
      var isSpamJudge = "spam"
      if (!(spamScore > 0)) {
        isSpamJudge = "ham"
      }
      (docid, isSpamlabel, spamScore, isSpamJudge)
    })
    .saveAsTextFile(args.output())
  }
}

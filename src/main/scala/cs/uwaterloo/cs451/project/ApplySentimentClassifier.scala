package ca.uwaterloo.cs451.project

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
  * Created by shipeng on 18-3-25.
  */
class Conf_Classifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ApplySentimentClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_Classifier(argv)
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())
    val conf = new SparkConf().setAppName("Classifier")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val modelFile = sc.textFile(args.model()+"/part-00000")
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val instances = textFile.map( line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      val label = tokens(1).trim()
      val features = tokens.slice(2, tokens.size).map(str => str.toInt)
      (docid, label, features)
    })

    val modelPara = modelFile.map( line => {
      val tokens = line.substring(1, line.length-1).split(",")
      val index = tokens(0).toInt
      val value = tokens(1).toDouble
      (index, value)
    }).collectAsMap()

    val broadcastPara = sc.broadcast(modelPara)

    val results = instances.map(instance => {
      def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        var weight = broadcastPara.value
        features.foreach(f=> if (weight.contains(f)) score += weight(f))
        score
      }
      val s = spamminess(instance._3)
      var label = "neg"
      if (s > 0) {
        label = "pos"
      }
      (instance._1, instance._2, s, label)
    })

    results.saveAsTextFile(args.output())
  }

}

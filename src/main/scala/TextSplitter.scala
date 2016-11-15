package cog

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.{Try, Success, Failure}
import models._
import utils._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe.TypeTag

object TextSplitter {
  final val APP_NAME = "TextSplitter"
  final val DEFAULT_MIN_PARTITIONS = 10

  def main(args: Array[String]): Unit = {
    var master: String = ""
    var withSample: Option[Int] = None
    var minPartitions = 10
    var path = "./data/files"
    var toPagesPath = "./data/pages"
    var toDocumentsPath = "./data/documents"
    var toSentencesPath = "./data/sentences"
    var fromPage: Option[Int] = Some(1)
    var toPage: Option[Int] = None

    args.sliding(2, 1).toList.collect {
      case Array("master", argsMaster: String) => master = argsMaster
      case Array("withSample", argsSample: String) => withSample = Try(argsSample.toInt).toOption
      case Array("minPartitions", p: String) => minPartitions = p.toInt
      case Array("path", p: String) => path = p
      case Array("toPagesPath", p: String) => toPagesPath = p
      case Array("toDocumentsPath", p: String) => toDocumentsPath = p
      case Array("toSentencesPath", p: String) => toSentencesPath = p
      case Array("fromPage", p: String) => fromPage = Try(p.toInt).toOption
      case Array("toPage", p: String) => toPage = Try(p.toInt).toOption
    }

    val conf = new SparkConf().setAppName(APP_NAME).setMaster(master = master)
    val session = SparkSession.builder.config(conf).getOrCreate()

    val files = readFiles(session.sparkContext, path, withSample, Some(minPartitions))

    val documents = files.mapPartitionsWithIndex((index: Int, iter: Iterator[(String, PortableDataStream)]) =>
      iter.map(i => (PDFReader.readAsPagesTextDocument(i._1, i._2.open), index))
    )

    val pagesDF = session.createDataFrame(documents.map(_._1).flatMap { case (pagesTextDocument: PagesTextDocument) =>
      pagesTextDocument.pages.map { case (index: Int, content: String) => Page(pagesTextDocument.fileName, index, content) }
    })

    val documentsDF = session.createDataFrame(documents.map(_._1).map { case (pagesTextDocument: PagesTextDocument) =>
      TextDocument(pagesTextDocument.fileName, pagesTextDocument.document)
    })

    /*
    val sentencesDF = session.createDataFrame(documents.map(_._1).flatMap { case (pagesTextDocument: PagesTextDocument) =>
      pagesTextDocument.pages.map { case (index: Int, content: String) => {
        SentenceSplitter.sentences(content).map {
          case (sentance: String) => Sentence(pagesTextDocument.fileName, index, sentance)
        }
      }}
    })
    */

    val sentences = documents.map(_._1).flatMap { case (pagesTextDocument: PagesTextDocument) =>
      pagesTextDocument.pages.map {
        case (i, page) => SentenceSplitter.sentences(page).map {
          case (s) => Sentence(pagesTextDocument.fileName, i, s)
        }
      }
    }.flatMap(row => row)

    // val sentenceDF = session.createDataFrame(sentences)
    //sentenceDF.collect().foreach { (row: ) => println(s"${row(0)} ${row(1)} ${row(2)}") }

    sentences.foreach { case (s) => println(s"${s.fileName} ${s.pageNumber} ${s.sentence.length}") }

    // TODO: Works
    //pagesDF.write.mode(SaveMode.Overwrite).parquet(toPagesPath)
    //documentsDF.write.mode(SaveMode.Overwrite).parquet(toDocumentsPath)

    // TODO: This works
    // documents.foreach { case (page: PagesTextDocument, i) => println(s"${page.fileName} ${page.pages.size} #${i}") }

    session.stop()
  }

  def readFiles(sc: SparkContext, path: String, withSample: Option[Int] = None, minPartitions: Option[Int] = None): RDD[(String, PortableDataStream)] = {
    if (withSample.isDefined) {
      sc.parallelize(sc.binaryFiles(path, minPartitions.getOrElse(DEFAULT_MIN_PARTITIONS)).takeSample(false, withSample.get))
    } else {
      sc.binaryFiles(path)
    }
  }
}






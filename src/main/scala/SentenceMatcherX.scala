package cog

import org.apache.log4j.{LogManager, Level}
import org.apache.spark.sql.{SparkSession, Row, SQLContext, SaveMode}
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import utils.LuceneQueryEscaper

trait SentenceMatcherXSparkApp extends App {
  final val logLevel = Level.WARN
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder
    .appName(this.getClass.getName)
    .getOrCreate()
}


case class Sentence(fileName: String, pageNumber: Int, sentenceIndex: Int, sentenceLength: Int, sentence: String, score: Option[Float] = Some(0.0.asInstanceOf[Float]))

case class SearchMatch(sentence: Sentence, sentences: Seq[Sentence])

case class DocumentMatch(sentenceA: Sentence, sentenceB: Sentence, score: Option[Float] = Some(0.0.asInstanceOf[Float]))

object SentenceMatcherX extends SentenceMatcherXSparkApp {
  final val STOP_WORDS = Seq[String](
    "kazalo", "KAZALO", "abstract", "ABSTRACT", "seznam", "SEZNAM",
    "literatura", "simboli", "strinjam", "objavo", "univerza", "dovoljujem"
  )
  final val STOP_SYMBOLS = Seq[String]("....", "----")
  final val MIN_SENTENCE_LENGTH = spark.conf.getOption("min_sentence_length").getOrElse("100").toInt
  final val MAX_SENTENCE_LENGTH = spark.conf.getOption("max_sentence_length").getOrElse("400").toInt
  final val SENTENCE_FUZZINESS = spark.conf.getOption("sentence_fuzziness").getOrElse("0.99").toFloat

  System.setProperty("index.store.mode", "disk")
  System.setProperty("lucenerdd.index.store.mode", "disk")

  val logger = LogManager.getLogger("SentenceMatcherX")
  val start = System.currentTimeMillis()

  import spark.implicits._

  lazy val luceneStopWords: String = (STOP_WORDS ++ STOP_SYMBOLS).map(s => s""""$s"""").mkString(" OR ")

  spark.sqlContext.udf.register("noStopWords", (text: String) => {
    val stopWordsExp = STOP_WORDS.map(s => s"""$s""").mkString("|")
    val specialChars = """\.\.\.\.|\-\-\-"""
    !text.matches(s".*(${specialChars}|${stopWordsExp}).*")
  })

  try {
    val documents = spark.read.load("data/documents")
    documents.createOrReplaceTempView("documents")

    logger.info(s"Documents count: ${documents.count()}")

    val sentences = spark.sql(
      s"""| SELECT fileName, inline(sentences)
          | FROM documents
          |
          | HAVING
          |   noStopWords(sentence) AND
          |   (sentenceLength BETWEEN $MIN_SENTENCE_LENGTH AND $MAX_SENTENCE_LENGTH)
          | LIMIT 500000
          | """.stripMargin)

    // WHERE fileName LIKE '%novica%'

    logger.info(s"Sentences count is ${sentences.count()}.")

    val rawSentencesRDD = sentences.select('fileName, 'pageNumber, 'sentenceIndex, 'sentenceLength, 'sentence)

    val sentencesRDD = LuceneRDD(rawSentencesRDD)
    sentencesRDD.cache()

    val luceneRDDCount: Long = sentencesRDD.count()
    logger.info(s"LuceneRDD count: ${luceneRDDCount}")

    val rowLinker: Row => String = {
      case row => {
        val fileName = row.getString(0)
        val sentence = LuceneQueryEscaper.escape(row.getString(4))
        val query = s"""-fileName:"$fileName" AND sentence:"$sentence"~$SENTENCE_FUZZINESS AND -sentence:($luceneStopWords)"""
        // logger.info(query)
        query
      }
    }

    val linked = sentencesRDD.linkDataFrame(rawSentencesRDD, rowLinker, luceneRDDCount.toInt)

    val linkedResults = spark.createDataFrame(linked.filter(_._2.nonEmpty).map {
      case (row: Row, docs: Array[SparkScoreDoc]) => {
        val sentence = Sentence(row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getString(4))
        val sentences = docs.map(doc => Sentence(
          doc.doc.textField("fileName").getOrElse("[none]"),
          doc.doc.numericField("pageNumber").getOrElse(-1).asInstanceOf[Int],
          doc.doc.numericField("sentenceIndex").getOrElse(-1).asInstanceOf[Int],
          doc.doc.numericField("sentenceLength").getOrElse(-1).asInstanceOf[Int],
          doc.doc.textField("sentence").getOrElse("[none]"),
          Some(doc.score)
        )).groupBy(_.fileName).map(_._2.head).asInstanceOf[Seq[Sentence]]

        SearchMatch(sentence, sentences)
      }
    }).as[SearchMatch]

    linkedResults.cache()

    /*
    linkedResults.foreach { searchMatch: SearchMatch => {
      println(s"${searchMatch.sentence.fileName} (${searchMatch.sentences.length}) ${searchMatch.sentence.sentence}")
      searchMatch.sentences.foreach { sentence =>
        println(s"\t ${sentence.fileName} [${sentence.score}] ${sentence.sentence}")
      }
    }
    }
    */

    val flatResults = linkedResults.flatMap(searchMatch =>
      searchMatch.sentences.map(s => {
        val Array(first, second) = Array(searchMatch.sentence, s).sortWith(_.fileName < _.fileName)
        DocumentMatch(first, second, s.score)
      }))
      .dropDuplicates("sentenceA", "sentenceB")

    flatResults.foreach { documentMatch: DocumentMatch =>
      println(s"${documentMatch.sentenceA.fileName} -> ${documentMatch.sentenceB.fileName} ${documentMatch.score.get}\n" +
        s"A = ${documentMatch.sentenceA.sentence}\n" +
        s"B = ${documentMatch.sentenceB.sentence}\n")

    }


  } finally {
    val end = System.currentTimeMillis()
    logger.info(s"Elapsed time: ${(end - start) / 1000.0} seconds.")
    spark.stop()
  }
}

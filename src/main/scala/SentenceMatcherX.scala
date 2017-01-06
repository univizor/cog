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

  val spark = SparkSession.builder.appName(this.getClass.getName).getOrCreate()
}


case class Sentence(fileName: String, pageNumber: Int, sentenceIndex: Int, sentenceLength: Int, sentence: String, score: Option[Float] = Some(0.0.asInstanceOf[Float]))

case class SearchMatch(sentence: Sentence, sentences: Seq[Sentence])

case class DocumentMatch(sentenceA: Sentence, sentenceB: Sentence, score: Option[Float] = Some(0.0.asInstanceOf[Float])) {
  def toMatchPoint: MatchPoint = MatchPoint(
    this.sentenceA.fileName, this.sentenceB.fileName,
    this.sentenceA.pageNumber, this.sentenceB.pageNumber,
    this.sentenceA.sentenceIndex, this.sentenceB.sentenceIndex,
    this.sentenceA.sentence.substring(0, 5), this.sentenceB.sentence.substring(0, 5),
    this.score.getOrElse {
      0.0.asInstanceOf[Float]
    }
  )
}

case class MatchPoint(fileNameA: String, fileNameB: String,
                      pageNumberA: Int, pageNumberB: Int,
                      sentenceIndexA: Int, sentenceIndexB: Int,
                      sentenceA: String, sentenceB: String,
                      score: Float)


object SentenceMatcherX extends SentenceMatcherXSparkApp {

  import DocumentMatch._

  final val STOP_WORDS = Seq[String](
    "kazalo", "KAZALO", "abstract", "ABSTRACT", "seznam", "SEZNAM", "plagiatorstvo", "Datum",
    "literatura", "simboli", "strinjam", "objavo", "univerza", "dovoljujem", "pravici", "zagovora"
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
          | -- WHERE fileName LIKE '%novica%'
          | HAVING
          |   (sentenceLength BETWEEN $MIN_SENTENCE_LENGTH AND $MAX_SENTENCE_LENGTH) AND noStopWords(sentence)
          | -- LIMIT 100000
          | """.stripMargin)

    // WHERE fileName LIKE '% novica % '

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

    val linked = sentencesRDD.linkDataFrame(rawSentencesRDD, rowLinker, 4)

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

    val flatResults = linkedResults.flatMap(searchMatch =>
      searchMatch.sentences.map(s => DocumentMatch(searchMatch.sentence, s, s.score)))
      .dropDuplicates("sentenceA", "sentenceB")


    flatResults.foreach { documentMatch: DocumentMatch =>
      println(s"${documentMatch.sentenceA.fileName} -> ${documentMatch.sentenceB.fileName} ${documentMatch.score.get}\n" +
        s"A = ${documentMatch.sentenceA.sentence}\n" +
        s"B = ${documentMatch.sentenceB.sentence}\n")
    }

    logger.info("Writing to \"data/points\".")

    flatResults.map(_.toMatchPoint)
      .write
      .mode(SaveMode.Overwrite)
      .save("data/points")


  } finally {
    val end = System.currentTimeMillis()
    logger.info(s"Elapsed time: ${(end - start) / 1000.0} seconds.")
    spark.stop()
  }
}

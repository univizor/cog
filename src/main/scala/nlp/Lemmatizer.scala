package nlp

import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream

import eu.hlavki.text.lemmagen.{LemmatizerFactory => HlavkiLemmatizerFactory}
import si.zitnik.research.lemmagen.{LemmagenFactory => ZitnikLemmagenFactory}

trait LikeLemmatizer {
  final val WORD_TOKEN_EXP = """([\p{L}_]{3,})""".r
  lazy val lemmatizer: Any = None

  def lemmatizeWord(word: String): String

  private def lemmatizeText(text: String): String = WORD_TOKEN_EXP.replaceAllIn(text, m => lemmatizeWord(m.group(1)))

  /**
    * Lemmatizes incoming text and retuns text where words are replaced with lemmas.
    * @param text
    * @return
    */
  def lemmatize(text: String) = {
    try {
      lemmatizeText(text)
    } catch {
      case e: Exception =>
        println(s"ERROR: Text: '${text}' Message: ${e.getMessage}")
        text
    }
  }
}

/**
  * This implementation uses Lemmagen4J for lemma extraction. Loading of this lemmatizer takes much more time,
  * even with GZIPed models. Once model is loaded it usually outperforms others - content-wise.
  */
trait ZitnikLemmatizer extends LikeLemmatizer {
  final val BUFFER_SIZE = 1024
  final val LEMMA_PATH = "/lemmas/lemmagenSLOModel.obj.gz"

  override lazy val lemmatizer: si.zitnik.research.lemmagen.impl.Lemmatizer = {
    ZitnikLemmagenFactory.readObject(new GZIPInputStream(new BufferedInputStream(getClass.getResourceAsStream(LEMMA_PATH), BUFFER_SIZE)))
      .asInstanceOf[si.zitnik.research.lemmagen.impl.Lemmatizer]
  }

  def lemmatizeWord(word: String) = lemmatizer.Lemmatize(word)
}

trait HlavkiLemmatizer extends LikeLemmatizer {
  final val MODEL_NAME = "mlteast-sl"
  override lazy val lemmatizer = HlavkiLemmatizerFactory.getPrebuilt(MODEL_NAME)

  def lemmatizeWord(word: String) = lemmatizer.lemmatize(word).toString
}

/**
  * Lemmatizer uses "HlavkiLemmatizer" as default implementation.
  */
object Lemmatizer extends HlavkiLemmatizer {}
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

trait ZitnikLemmatizer extends LikeLemmatizer {
  override lazy val lemmatizer: si.zitnik.research.lemmagen.impl.Lemmatizer = {
    ZitnikLemmagenFactory.readObject(new GZIPInputStream(new BufferedInputStream(getClass.getResourceAsStream(s"/lemmas/lemmagenSLOModel.obj.gz"))))
      .asInstanceOf[si.zitnik.research.lemmagen.impl.Lemmatizer]
  }

  def lemmatizeWord(word: String) = lemmatizer.Lemmatize(word)
}

trait HlavkiLemmatizer extends LikeLemmatizer {
  override lazy val lemmatizer = HlavkiLemmatizerFactory.getPrebuilt("mlteast-sl")

  def lemmatizeWord(word: String) = lemmatizer.lemmatize(word).toString
}

object Lemmatizer extends HlavkiLemmatizer {}
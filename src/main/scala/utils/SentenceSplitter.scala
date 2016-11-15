package utils

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import scala.util.{Try}

object SentenceSplitter {
  def sentences(content: String, modelPath: Option[String] = Some("/models/si_sentences.bin")): Array[String] = {
    val model = new SentenceModel(getClass.getResourceAsStream(modelPath.get))
    val sentenceDetector = new SentenceDetectorME(model)
    val sentencesText = Try(sentenceDetector.sentDetect(content)).toOption
    sentencesText.getOrElse(Array.empty[String])
  }
}

package utils

object TextMatcher {
  def matchOf(text: String, expression: String): Int = {
    expression.r.findFirstMatchIn(text).map(_.start).getOrElse(-1)
  }

  def firstOf(text: String, expression: String) = {
    expression.r.findFirstIn(text).orNull
  }
}

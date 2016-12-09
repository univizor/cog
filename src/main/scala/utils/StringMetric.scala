package utils

import com.rockymadden.stringmetric.similarity._

/**
  * Computes similarity between two strings.
  * - http://research.ijcaonline.org/volume68/number13/pxc3887118.pdf
  * - https://github.com/rockymadden/stringmetric
  */
object StringMetric {

  def distance(a: String, b: String): Double = {
    if (a == "" || b == "") return 0.0
    DiceSorensenMetric(1).compare(a, b).getOrElse(0.0)
  }
}
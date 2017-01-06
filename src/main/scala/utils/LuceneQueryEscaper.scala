package utils

import org.apache.lucene.queryparser.classic.{QueryParser, QueryParserBase}

object LuceneQueryEscaper {
  def escape(text: String): String = QueryParserBase.escape(text)
}

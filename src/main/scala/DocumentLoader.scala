import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, Metadata}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, FileFormat, HadoopFsRelation}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet
import utils.PDFReader

trait CogSparkApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  var spark = SparkSession.builder.appName("DocumentLoader").getOrCreate()
}

object DocumentLoader extends CogSparkApp {
  val Array(documentsPath, targetPath) = Array(args(0), args(1))

  try {
    println(documentsPath)
    println(targetPath)

    val files = spark.read.format("utils.sources.CogPDF").load(documentsPath)

    files.show(10, false)

  } finally {
    spark.stop()
  }
}
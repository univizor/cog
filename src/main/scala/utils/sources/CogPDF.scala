package utils.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class CogPDF extends FileFormat {

  def shortName(): String = "cog"

  override def toString: String = "CogPDF"

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    Some(StructType(List(
      StructField("filePath", StringType, nullable = true),
      StructField("length", LongType, nullable = true),
      StructField("text", StringType, nullable = true)
    )))
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    throw new Exception("You can't write to CogPDF")
  }

  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    pf => Iterator {
      /*
      val hdfsPath = new Path(pf.filePath)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val document = PDFReader.read(pf.filePath, fs.open(hdfsPath))._1
      */

      InternalRow(
        UTF8String.fromString(pf.filePath),
        pf.length,
        UTF8String.fromString(s"I was here ${pf.start}")
      )
    }
  }
}

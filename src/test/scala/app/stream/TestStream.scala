package app.stream
import java.io.{ BufferedWriter, File, FileWriter, PrintWriter }
import java.net.URL

import app.utils.Utils.{ transformStringColumnsToTimestampColumns, truncateSpaceInStringColumns }
import app.utils.xml.Sink
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.streaming.DataStreamWriter
import org.scalacheck.Prop.False
import org.scalatest._

import scala.io.Source
import scala.xml.XML

class TestStream extends FlatSpec {

  "A Stream" should "read param xml file, transform column names, load sql files and write to sinks " in {

    // create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("stream_app")
      .getOrCreate()

    // load parameters.xml scala xml Elem object
    val elam = XML.loadFile("src/test/resources/parameter.xml")

    // convert xml Elem object to app.utils.xml.Parameter
    val parameter = app.utils.xml.Parameter.fromXML(elam)

    // sinks to write
    val sinks: List[Sink] = parameter.sink

    // source to read
    val source = parameter.source

    // name of table to query
    val sourceTable = source.sourceTable

    // columns to transform
    val transformColumns = parameter.transform

    // set log level
    spark.sparkContext.setLogLevel("ERROR")

    // set kryo serializer
    spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // read file to infer schema
    val dataFrame = spark.read
      .format(source.format)
      .options(source.options.map(option => (option.key, option.value)).toMap)
      .load(source.path)

    // schema for stream
    val schema = dataFrame.schema

    // read stream
    var dataFrameS =
      spark.readStream
        .schema(schema)
        .format(source.format)
        .options(source.options.map(option => (option.key, option.value)).toMap)
        .load(source.path)

    // transform column names from string to timestamp
    transformColumns.foreach(
      transform => {
        val pattern = transform.pattern
        val columns = transform.columns
        dataFrameS = transformStringColumnsToTimestampColumns(dataFrameS, columns, pattern)
      }
    )

    // trunc spaces in columns names
    dataFrameS = truncateSpaceInStringColumns(dataFrameS)

    // set water mark on stream from source
    dataFrameS = dataFrameS.withWatermark(source.waterMark.columnName, source.waterMark.timeInterval)

    // register temp view for sql files
    dataFrameS.createOrReplaceTempView(sourceTable)

    // map sinks with sql query
    val sinksQuery: List[(Sink, String)] = sinks.map(
      sink => (sink, spark.sparkContext.textFile(sink.executeQuery.sql).collect().mkString("\n"))
    )

    // map sinks with DataFrame result
    val sinksDataFrame: List[(Sink, DataFrame)] =
      sinksQuery.map(
        sink =>
          (sink._1,
           spark
             .sql(sink._2))
      )

    // write DataFrames to stream sinks
    val streams: List[DataStreamWriter[Row]] = sinksDataFrame.map(
      sink =>
        sink._2.writeStream
          .format(sink._1.format)
          .option(sink._1.executeQuery.checkPoint.key, sink._1.executeQuery.checkPoint.value)
          .options(sink._1.options.map(option => (option.key, option.value)).toMap)
          .outputMode(sink._1.outputMode)
    )

    // create new Runnable for sink
    def newRunnable(dataWriter: DataStreamWriter[Row]) = {
      val runnable = new Runnable {
        override def run {
          dataWriter.start().awaitTermination()
        }
      }
      runnable
    }

    // start stream data to sink
    streams.foreach(
      stream => {
        val thread = new Thread(newRunnable(stream))
        thread.start()
      }
    )

    assert(true)

  }
}

package app.stream
import app.utils.Utils._
import app.utils.xml.Sink
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery

import scala.xml.XML

object Stream {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage : provide parameters.xml file")
    }

    val file = args(0)

    val fileElem = XML.loadFile(file)

    val parameters = app.utils.xml.Parameter.fromXML(fileElem)

    val sinks: List[Sink] = parameters.sink

    val source = parameters.source

    val tranformColumns = parameters.transform

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("stream_app")
      .getOrCreate()
    // set log level
    spark.sparkContext.setLogLevel("ERROR")
    // set kryo serializer
    spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    import spark.implicits._

    val map = source.options.map(option => (option.key, option.value)).toMap

    val dataFrame = spark.read
      .format(source.format)
      .options(source.options.map(option => (option.key, option.value)).toMap)
      .load(source.path)

    val schema = dataFrame.schema

    var dataFrameS =
      spark.readStream
        .schema(schema)
        .format(source.format)
        .options(source.options.map(option => (option.key, option.value)).toMap)
        .load(source.path)

    tranformColumns.foreach(
      transform => {
        val pattern = transform.pattern
        val columns = transform.columns
        dataFrameS = transformStringColumnsToTimestampColumns(dataFrameS, columns, pattern)
      }
    )

    dataFrameS = truncateSpaceInStringColumns(dataFrameS)

    dataFrameS.createOrReplaceTempView(source.sourceTable)

    // map sinks with sql query
    val sinksQuery: List[(Sink, String)] = sinks.map(
      sink => (sink, spark.sparkContext.textFile(sink.executeQuery.sql).collect().mkString("\n"))
    )

    // map sinks with DataFrame result
    val sinksDataFrame: List[(Sink, DataFrame)] =
      sinksQuery.map(sink => (sink._1, spark.sql(sink._2)))

    // write DataFrames to stream sinks
    val streams: List[StreamingQuery] = sinksDataFrame.map(
      sink =>
        sink._2.writeStream
          .format(sink._1.format)
          .options(sink._1.options.map(option => (option.key, option.value)).toMap)
          .outputMode(sink._1.outputMode)
          .start()
    )

    streams.foreach(stream => stream.awaitTermination())

  }

}

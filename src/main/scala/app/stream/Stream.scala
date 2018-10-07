package app.stream
import app.utils.Utils._
import app.utils.xml.Sink
import org.apache.spark.sql._

import scala.xml.XML

object Stream {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage : provide parameters.xml file")
    }

    val file = args(0)

    val fileElem = XML.loadFile(file)

    val parameters = app.utils.xml.Parameter.fromXML(fileElem)

    val sink = parameters.sink

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
        dataFrameS=transformStringColumnsToTimestampColumns(dataFrameS, columns, pattern)
      }
    )

    dataFrameS = truncateSpaceInStringColumns(dataFrameS)

    // register temp view for query
    dataFrameS.createOrReplaceTempView(parameters.sqlFileLocation.tableName)

    // load sql file
    val sql: Array[String] = spark.sparkContext.textFile(parameters.sqlFileLocation.url).collect()

    sql.foreach(query => dataFrameS = spark.sql(query))

    val query =
      dataFrameS.writeStream
        .format(sink.format)
        .options(sink.options.map(option => (option.key, option.value)).toMap)
        .outputMode(sink.outputMode)
        .start()

    query.awaitTermination()

  }

}

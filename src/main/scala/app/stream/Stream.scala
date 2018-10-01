package app.stream
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.{ month, _ }
import org.apache.spark.sql.streaming.OutputMode
import scalafx.application.JFXApp
import app.utils._

object Stream {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage : provide readStreamPath and writeStreamPath arguments")
    }
    // read and write path
    val readStreamPath  = args(0)
    val writeStreamPath = args(1)

    // timestamp patterns
    val fromPattern1 = "MM/dd/yyyy"
    val toPattern1   = "yyyy-MM-dd"

    val fromPattern2 = "MM/dd/yyyy hh:mm:ss aa"
    val toPattern2   = "MM/dd/yyyy hh:mm:ss aa"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("stream_app")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    import spark.implicits._

    val dataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load(readStreamPath)

    val schema = dataFrame.schema

    val dataFrameS =
      spark.readStream
        .schema(schema)
        .format("csv")
        .option("header", "true")
        .load(readStreamPath)

    import app.utils._
    // transform string columns with patter "MM/dd/yyyy"  to timestamp
    var columnsToTransform          = Set(("Call Date", "CallDateTs"), ("Watch Date", "WatchDateTs"))
    var patternForColumnsToTranform = "MM/dd/yyyy"
    var dataFrameSTs = trnsformStringColumnsToTimestampColumns(dataFrameS,
                                                               columnsToTransform,
                                                               patternForColumnsToTranform)
    //transform string columns with pattern "MM/dd/yyyy hh:mm:ss aa" to timestamp
    columnsToTransform = Set(
      ("Entry DtTm", "EntryDtTs"),
      ("Received DtTm", "ReceivedDtTs"),
      ("Dispatch DtTm", "DispatchDtTs"),
      ("Response DtTm", "ResponseDtTs"),
      ("On Scene DtTm", "OnSceneDtTs"),
      ("Transport DtTm", "TransportDtTs"),
      ("Hospital DtTm", "HospitalDtTs"),
      ("Available DtTm", "AvailableDtTs")
    )
    patternForColumnsToTranform = "MM/dd/yyyy hh:mm:ss aa"
    dataFrameSTs = trnsformStringColumnsToTimestampColumns(
      dataFrameSTs,
      columnsToTransform,
      patternForColumnsToTranform
    )
    val winCount = dataFrameSTs
      .groupBy(
        month($"CallDateTs") as "month"
      )
      .count()

    val query = winCount.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    dataFrameSTs.select(month($"CallDateTs"))

    val query1 = winCount.writeStream
      .format("parquet")
      .option("path", writeStreamPath)
      .outputMode(OutputMode.Complete())
      .start()

    new JfxApp().pieChart(
      spark.read.parquet(writeStreamPath).groupBy($"mounth").agg(sum($"count"))
    )
    query.awaitTermination()

  }

}

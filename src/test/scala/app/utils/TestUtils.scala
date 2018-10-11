package app.utils
import app.SparkSessionTestWrapper
import app.utils.xml._
import org.scalatest._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import app.utils.Utils._

import scala.xml.XML
class TestUtils extends FlatSpec with SparkSessionTestWrapper {

  "A utils" should "tranform string dataframe columns with timestamp to timestamp dataframe columns" in {

    val schema = StructType(
      List(
        StructField("dayTime", StringType, true),
        StructField("secondTime", StringType, true)
      )
    )
    val expectedSchema = StructType(
      List(
        StructField("dayTimeTs", TimestampType, true),
        StructField("secondTimeTs", TimestampType, true)
      )
    )

    val data          = Seq(Row("dayTime", "2018/10/1"), Row("secondTime", "2018/10/1 02:07:56 AM"))
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
    val dataFrame     = spark.createDataFrame(rdd, schema)

    val fromPattern1 = "MM/dd/yyyy"
    val fromPattern2 = "MM/dd/yyyy hh:mm:ss aa"
    val frame1       = transformStringColumnsToTimestampColumns(dataFrame, List(new Column("dayTime", "dayTimeTs")), fromPattern1)
    val frame2 =
      transformStringColumnsToTimestampColumns(frame1, List(new Column("secondTime", "secondTimeTs")), fromPattern2)
    assert(expectedSchema.treeString === frame2.schema.treeString)

  }

  "A Utils" should "write and read xml parameter file" in {

    val sourceOption1 = new Option("header", "true")
    val sourceOption2 = new Option("inferSchema", "true")
    val waterMark     = new WaterMark("CallDateTs", "1 day")
    val source        = new Source("csv", "src/test/resources/source", List(sourceOption1, sourceOption2), "fire_calls", waterMark)

    val sink1Option = new Option("path", "src/test/resources/sink/group_by_day_stream")
    val sink2Option = new Option("path", "src/test/resources/sink/group_by_month_stream")
    val sink3Option = new Option("path", "src/test/resources//sink/group_by_year_stream")

    val sink1checkPoint = new CheckPoint("checkpointLocation", "src/test/resources/checkpoint_sink1")
    val sink2checkPoint = new CheckPoint("checkpointLocation", "src/test/resources/checkpoint_sink2")
    val sink3checkPoint = new CheckPoint("checkpointLocation", "src/test/resources/checkpoint_sink3")

    val sink1Query =
      new ExecuteQuery(
        _sqlFile = "src/test/resources/sql/streamoperation/group_by_day_stream.sql",
        _checkPoint = sink1checkPoint
      )
    val sink2Query =
      new ExecuteQuery(
        _sqlFile = "src/test/resources/sql/streamoperation/group_by_month_stream.sql",
        _checkPoint = sink2checkPoint
      )
    val sink3Query =
      new ExecuteQuery(
        _sqlFile = "src/test/resources/sql/streamoperation/group_by_year_stream.sql",
        _checkPoint = sink3checkPoint
      )

    val sink1 = new Sink("parquet", "append", List(sink1Option), sink1Query)
    val sink2 = new Sink("parquet", "append", List(sink2Option), sink2Query)
    val sink3 = new Sink("parquet", "append", List(sink3Option), sink3Query)

    val transform1Column1 = new Column(_from = "Call Date", _to = "CallDateTs")
    val transform1Column2 = new Column(_from = "Watch Date", _to = "WatchDateTs")
    val transform1        = new TransformColumnName("MM/dd/yyyy", List(transform1Column1, transform1Column2))

    val transform2Column1 = new Column("Entry DtTm", "EntryDtTs")
    val transform2Column2 = new Column("Received DtTm", "ReceivedDtTs")
    val transform2Column3 = new Column("Dispatch DtTm", "DispatchDtTs")
    val transform2Column4 = new Column("Response DtTm", "ResponseDtTs")
    val transform2Column5 = new Column("On Scene DtTm", "OnSceneDtTs")
    val transform2Column6 = new Column("Transport DtTm", "TransportDtTs")
    val transform2Column7 = new Column("Hospital DtTm", "HospitalDtTs")
    val transform2Column8 = new Column("Available DtTm", "AvailableDtTs")
    val transform2 =
      new TransformColumnName(
        "MM/dd/yyyy hh:mm:ss aa",
        List(
          transform2Column1,
          transform2Column2,
          transform2Column3,
          transform2Column4,
          transform2Column5,
          transform2Column6,
          transform2Column7,
          transform2Column8
        )
      )

    val result = new Parameter(source, List(sink1, sink2, sink3), List(transform1, transform2))

    val elem = XML.loadString(new scala.xml.PrettyPrinter(200, 2).formatNodes(result.toXML))

    XML.save("src/test/resources/parameter.xml", elem, "UTF-8", xmlDecl = true)

    val expected = Parameter.fromXML(XML.loadFile("src/test/resources/parameter.xml"))

    assert(result.toXML === expected.toXML)

  }

}

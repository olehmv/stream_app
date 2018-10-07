package app.utils
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
    val frame1 = transformStringColumnsToTimestampColumns(dataFrame,
                                                          List(new Column("dayTime", "dayTimeTs")),
                                                          fromPattern1)
    val frame2 =
      transformStringColumnsToTimestampColumns(frame1,
                                               List(new Column("secondTime", "secondTimeTs")),
                                               fromPattern2)
    assert(expectedSchema.treeString === frame2.schema.treeString)

  }

  "A Utils" should "write and read xml parameter file" in {

    val sourceOption1 = new Option("header", "true")
    val sourceOption2 = new Option("inferSchema", "true")
    val source        = new Source("csv", "D:\\csv_files", List(sourceOption1, sourceOption2))

    val sinkOption1 = new Option("path", "D:\\parquet_files")
    val sinkOption2 = new Option("checkpointLocation", "src\\test\\resources\\checkpoint")
    val sink        = new Sink("parquet", "append", List(sinkOption1, sinkOption2))

    val transform1Column1 = new Column(_from = "Call Date", _to = "CallDateTs")
    val transform1Column2 = new Column(_from = "Watch Date", _to = "WatchDateTs")
    val transform1        = new Transform("MM/dd/yyyy", List(transform1Column1, transform1Column2))

    val transform2Column1  = new Column("Entry DtTm","EntryDtTs")
    val transform2Column2  = new Column("Received DtTm","ReceivedDtTs")
    val transform2Column3  = new Column("Dispatch DtTm", "DispatchDtTs")
    val transform2Column4  = new Column("Response DtTm", "ResponseDtTs")
    val transform2Column5  = new Column("On Scene DtTm", "OnSceneDtTs")
    val transform2Column6  = new Column("Transport DtTm", "TransportDtTs")
    val transform2Column7  = new Column("Hospital DtTm", "HospitalDtTs")
    val transform2Column8 = new Column("Available DtTm", "AvailableDtTs")
    val transform2 =
      new Transform(
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

    val url = new SqlFileLocation("src\\test\\resources\\transformation.sql", "fire_calls")

    val result = new Parameter(source, sink, List(transform1, transform2), url)

    val elem = XML.loadString(new scala.xml.PrettyPrinter(80, 2).formatNodes(result.toXML))

    XML.save("src\\test\\resources\\parameter.xml", elem, "UTF-8", xmlDecl = true)

    val expected = Parameter.fromXML(XML.loadFile("src\\test\\resources\\parameter.xml"))

    assert(result.toXML === expected.toXML)

  }

}

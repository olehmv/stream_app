package app.utils
import org.scalatest._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row }
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
    val frame1 = trnsformStringColumnsToTimestampColumns(dataFrame,
                                                         Set(("dayTime", "dayTimeTs")),
                                                         fromPattern1)
    val frame2 = trnsformStringColumnsToTimestampColumns(frame1,
                                                         Set(("secondTime", "secondTimeTs")),
                                                         fromPattern2)
    assert(expectedSchema.treeString === frame2.schema.treeString)

  }

}

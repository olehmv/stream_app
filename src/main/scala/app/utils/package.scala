package app

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._

import scala.runtime.Nothing$
package object utils {

  def trnsformStringColumnsToTimestampColumns(
      dataFrame: DataFrame,
      fromToColumnsNamesListOfTuples: Set[(String, String)],
      pattern: String
  ): DataFrame = {

    var dataFrameTs: DataFrame = dataFrame

    fromToColumnsNamesListOfTuples.map(
      tuple =>
        dataFrameTs = dataFrameTs
          .withColumn(tuple._2, unix_timestamp(dataFrame(tuple._1), pattern).cast("timestamp"))
          .drop(tuple._1)
    )

    dataFrameTs
  }
}

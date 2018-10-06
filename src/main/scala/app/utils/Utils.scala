package app.utils

import app.utils.xml.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.unix_timestamp

/**
  * Utils object for transformation
  *
  */
object Utils {

  /**
    * Transform String columns with timestamp to TimeStamp columns type
    *
    * @param dataFrame
    * @param fromToColumnsNamesList
    * @param pattern
    * @return DataFrame
    */
  def transformStringColumnsToTimestampColumns(
      dataFrame: DataFrame,
      fromToColumnsNamesList: List[Column],
      pattern: String
  ): DataFrame = {

    var dataFrameTs: DataFrame = dataFrame

    fromToColumnsNamesList.map(
      column =>
        dataFrameTs = dataFrameTs
          .withColumn(column.to, unix_timestamp(dataFrame(column.from), pattern).cast("timestamp"))
          .drop(column.from)
    )
    dataFrameTs
  }

  /**
    * Transform string columns with spaces to string columns without spaces
    *
    * @param dataFrame
    * @return DataFrame
    */
  def truncateSpaceInStringColumns(dataFrame: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    var dataFrameTrancColumns = dataFrame
    val space                 = " "
    val columnsWithSpace      = dataFrameTrancColumns.columns.filter(column => column.contains(space))

    columnsWithSpace
      .map { column =>
        var columnNoSpace = column.filterNot((x: Char) => x.isWhitespace)
        dataFrameTrancColumns = dataFrameTrancColumns
          .withColumn(columnNoSpace, col(column))
          .drop(column)
      }
    dataFrameTrancColumns

  }

}

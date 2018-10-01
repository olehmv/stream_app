package app.utils

import org.apache.spark.sql.{ DataFrame, Row }
import scalafx.application.JFXApp

class JfxApp extends JFXApp {

  def pieChart(dataFrame: DataFrame): Unit = {

//    val keyValue = dataFrame.columns
//      .map(
//        column =>
//          (dataFrame.select(column).toS, dataFrame.groupBy(column).count().toString().toLong)
//      )
//      .toMap

    val keyValue =
      dataFrame.rdd.collect().map(row => (row(0).toString, row(1).toString.toLong))

    stage = new JFXApp.PrimaryStage {

      import scalafx.scene.Scene

      title = "Auto Pie Chart "
      scene = new Scene(500, 500) {

        import scalafx.scene.chart.PieChart

        root = new PieChart {

          import scalafx.collections.ObservableBuffer

          title = "Columns Pie Chart"
          clockwise = false
          clockwise = false
          data = ObservableBuffer(keyValue.toList.map {
            case (x: String, y: Long) => PieChart.Data(x, y)
          })
        }
      }
    }
  }
}

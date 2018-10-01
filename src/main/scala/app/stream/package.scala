package app

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.unix_timestamp

package object stream {}

//    val dataFrameSTs = dataFrameS
//      .withColumn("CallDateTs",
//                  unix_timestamp(dataFrameS("Call Date"), fromPattern1).cast("timestamp"))
//      .drop("Call Date")
//      .withColumn("WatchDateTs",
//                  unix_timestamp(dataFrameS("Watch Date"), fromPattern1).cast("timestamp"))
//      .drop("Watch Date")
//      .withColumn("ReceivedDtTs",
//                  unix_timestamp(dataFrameS("Received DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Received DtTm")
//      .withColumn("EntryDtTs",
//                  unix_timestamp(dataFrameS("Entry DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Entry DtTm")
//      .withColumn("DispatchDtTs",
//                  unix_timestamp(dataFrameS("Dispatch DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Dispatch DtTm")
//      .withColumn("ResponseDtTs",
//                  unix_timestamp(dataFrameS("Response DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Response DtTm")
//      .withColumn("OnSceneDtTs",
//                  unix_timestamp(dataFrameS("On Scene DtTm"), fromPattern2).cast("timestamp"))
//      .drop("On Scene DtTm")
//      .withColumn("TransportDtTs",
//                  unix_timestamp(dataFrameS("Transport DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Transport DtTm")
//      .withColumn("HospitalDtTs",
//                  unix_timestamp(dataFrameS("Hospital DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Hospital DtTm")
//      .withColumn("AvailableDtTs",
//                  unix_timestamp(dataFrameS("Available DtTm"), fromPattern2).cast("timestamp"))
//      .drop("Available DtTm")

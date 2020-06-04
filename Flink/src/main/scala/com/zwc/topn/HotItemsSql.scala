package com.zwc.topn

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
/*
只使用Flink SQL实现TopN需求
 */
object HotItemsSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream: DataStream[UserBehavior] = env
      .readTextFile("D:\\Git_WorkSpace\\Flink\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)

    tableEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    val result = tableEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        | SELECT *,
        |  ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
        | FROM
        | (SELECT count(itemId) as icount,
        | TUMBLE_START(ts, INTERVAL '1' HOUR)  as windowEnd
        | FROM t GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), itemId) topn)
        |WHERE row_num <= 5
        |""".stripMargin
    )
    result.toRetractStream[(Long, Timestamp, Long)]
      .print()

    env.execute()
  }
  // 把数据需要ETL成UserBehavior类型
  case class UserBehavior (userId: Long,
                           itemId: Long,
                           categoryId: Int,
                           behavior: String,
                           timestamp: Long
                          )
}
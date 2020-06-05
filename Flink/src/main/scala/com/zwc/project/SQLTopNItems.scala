package com.zwc.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
/* Flink-SQL API 实现 实时热门商品统计topN*/
object SQLTopNItems {
  // 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("UserBehavior.csv绝对路径")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp) // 数据集提前按照时间戳排了一下序

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // top n 需求只有blink planner支持
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    // ROW_NUMBER 排好序的行号 使用窗口结束时间windowEnd分区/分流/分组 根据每个窗口浏览量icount排序
    // 命名一个计数值icount  滚动窗口结束时间ts 窗口大小 1个小时
    // 使用1小时滚动窗口 + itemId 来分组
    // row_num <= 3 取出前三名
    tableEnv
      .sqlQuery(
        """
          |SELECT icount, windowEnd, row_num
          |FROM (
          |       SELECT icount,
          |              windowEnd,
          |              ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
          |       FROM
          |         (SELECT count(itemId) as icount,
          |             HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
          |          FROM t GROUP BY HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR), itemId) as topn
          |)
          |WHERE row_num <= 3
          |""".stripMargin
      )
      // (itemId, ts, rank)
      .toRetractStream[(Long, Timestamp, Long)]
      .print()
    env.execute()
  }
}

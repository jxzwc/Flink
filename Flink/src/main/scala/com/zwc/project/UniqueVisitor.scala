package com.zwc.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.Set
// TODO 统计UV
// 独立访客：一段时间有多少用户访问了网站，涉及到去重

object UniqueVisitor {
  // 样例类
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
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.userId, "key"))
      .keyBy(_._2)  // 将所有数据keyBy到同一条流
      .timeWindow(Time.hours(1))   // 开窗
      .process(new ComputeUV)     // 聚合

    stream.print("stream")
    env.execute()
  }

  class ComputeUV extends ProcessWindowFunction[(Long, String), String, String, TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String)],
                         out: Collector[String]): Unit = {
      var s: Set[Long] = Set()

      for(e <- elements) {
        s += e._1
      }
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV数据是:" + s.size)
    }
  }
}

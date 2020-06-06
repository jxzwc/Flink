package com.zwc.project

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * TODO 实时流量统计
 * TODO 基本需求 从web服务器的日志中，统计实时的访问流量
 * 统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
 * TODO 解决思路
 * 将apache服务器日志中的时间，转换为时间戳，作为Event Time
 * 构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
 * TODO 代码实现
 * 我们现在要实现的模块是“实时流量统计”。
 * 对于一个电商平台而言，用户登录的入口流量、不同页面的访问流量都是值得分析的重要数据，
 * 而这些数据，可以简单地从web服务器的日志中提取出来。我们在这里实现最基本的“页面浏览数”的统计，
 * 也就是读取服务器日志中的每一行log，统计在一段时间内用户访问url的次数。
 *
 * 具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。
 * 可以看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此前的代码
 */
object TopNUrl {

  case class  ApacheLogEvent(ip: String,
                             userId: String,
                             eventTime: Long,
                             method: String,
                             url: String)
  case class UrlViewCount(url: String,
                          windowEnd: Long,
                          count: Long)

  def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("apachelog.txt绝对路径")
      .map(line => {
        val arr = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), ts, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResult)
        .keyBy(_.windowEnd)
        .process(new TopNUrls(5))

    stream.print()

    env.execute()
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow]{
    override def process(key: String,
                         context: Context, elements: Iterable[Long],
                         out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class TopNUrls(val topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("url-state", Types.of[UrlViewCount])
    )
    override def processElement(value: UrlViewCount,
                                ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(urlView <- urlState.get) {
        allUrlViews += urlView
      }
      urlState.clear()
      val sortedUrlViews = allUrlViews.sortBy(-_.count).take(topSize)
      val result = new StringBuilder
      result
        .append("=======================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for(i <- sortedUrlViews.indices) {
        val cur = sortedUrlViews(i)
        result
          .append("Nomber.")
          .append(i + 1)
          .append("：")
          .append(" URL = ")
          .append(cur.url)
          .append("流量 = ")
          .append(cur.count)
          .append("\n")
      }
      result
          .append("=======================================\n\n\n")

      Thread.sleep(10)
      out.collect(result.toString)
    }
  }

}

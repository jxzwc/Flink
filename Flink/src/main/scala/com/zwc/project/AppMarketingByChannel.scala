package com.zwc.project

import java.sql.Timestamp
import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
/**
 * TODO APP分渠道数据统计
 */
object AppMarketingByChannel {

  // TODO 样例类
  case class MarketingUserBehavior(userId: String,
                                   behavior: String,
                                   channel: String,
                                   ts: Long)

  // TODO 伪造数据源
  class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior]{
    var running = true

    // fake渠道信息
    val channelSet = Seq("AppStore", "XiaomiStore")
    // fake用户行为信息
    val behaviorTypes = Seq("BROWSE", "CLICK", "UNINSTALL", "INSTALL")

    var random = new Random()

    // 定义如何发送数据的逻辑
    override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
      while(running) {
        // UUID产生一个唯一的字符串，本质就是哈希
        // 伪造一个userId 使用UUID库产生一个独一无二的userId UUID 用在那里？分配全局唯一用户ID
        val userId = UUID.randomUUID().toString
        // behaviorTypes.size 序列长度
        val behaviorType = behaviorTypes(random.nextInt(behaviorTypes.size))
        val channel = channelSet(random.nextInt(channelSet.size))
        // 产生时间戳 ts
        val ts = Calendar.getInstance().getTimeInMillis

        // 往外发送数据
        ctx.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))

        Thread.sleep(10)
      }
    }

    //  取消任务要做的 把running 赋值为false
    override def cancel(): Unit = running = false
  }

  def main(args: Array[String]): Unit = {
    // TODO 分渠道统计数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // map之后的数据样式：((XiaomiStore,BROWSE),1)  ((AppStore,CLICK),1) ((XiaomiStore,INSTALL),1)
    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r => {   // 对于每一条数据map成元组
        ((r.channel, r.behavior), 1L)
      })
      .keyBy(_._1) // 分流 使用元组进行keyBy
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountByChannel)

    stream.print("分渠道统计数据")

    env.execute()
  }

  // 泛型第一个参数是输入类型 输入类型在map的时候变了是一个元组
  // 泛型第二个参数是输出
  // 泛型第三个参数是key
  // 泛型第四个参数是TimeWindow

  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long),
                                                              (String, Long, Timestamp),
                                                              (String, String), TimeWindow]{
    override def process(key: (String, String),
                         context: Context,
                         elements: Iterable[((String, String), Long)],
                         out: Collector[(String, Long, Timestamp)]): Unit = {

      out.collect((key._1, elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}

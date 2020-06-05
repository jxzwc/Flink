package com.zwc.project

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/* Flink-Table API 实现 实时热门商品统计topN*/

object TopNItems {

  // 定义样例类 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  // 窗口聚合输出的样例类
  // 某个窗口中，某个商品的pv次数
  case class ItemViewCount(itemId: Long,
                           windowEnd: Long, // 窗口结束时间
                           count: Long)
  def main(args: Array[String]): Unit = {

    // 创建一个运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 为了打印方便设置并行度为1
    env.setParallelism(1)
    // 设置Time类型为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //TODO 读取数据源 处理数据
    val stream = env
      .readTextFile("UserBehavior.csv绝对路径")
      .map(line => {
        val arr = line.split(",")
        // 封装样例类，返回一个UserBehavior
        UserBehavior( arr(0).toLong,
          arr(1).toLong,
          arr(2).toInt,
          // 切割完就是String 不用toString转了
          arr(3),
          // 转成ms
          arr(4).toLong * 1000 )
      })
      // 行为是pv的，过滤出点击事件
      .filter(_.behavior.equals("pv"))
      // 指定时间戳和水位线，这里这里我们已经知道了数据集的时间戳是单调递增的了。
      // 分配升序时间戳
      .assignAscendingTimestamps(_.timestamp)
      // 根据商品Id分流
      .keyBy(_.itemId)
      // 开窗操作 滑动窗口1个小时 滑动距离5分钟
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 窗口计算操作
      .aggregate(new CountAvg, new WindowResult)  // --> 合流成DataStream
      // 根据窗口结束时间分流
      .keyBy(_.windowEnd)
      // 求点击量前3名的商品
      .process(new TopN(3))

    //TODO 打印结果
    stream.print("商品-topN")
    // TODO 执行
    env.execute("Hot Items Job")
  }

  // TODO 增量聚合函数逻辑编写  COUNT统计的聚合函数实现，每出现一条记录就加一
  // 第一个参数 输入样例类
  // 第二个参数 累加器 Long
  // 第三个参数 输出 Long
  // 聚合的元素属于同一个Key和Window
  // 窗口闭合时，增量聚合函数将聚合结果发送给全窗口聚合函数
  class CountAvg extends AggregateFunction[UserBehavior, Long, Long]{

    // 创建累加器
    override def createAccumulator(): Long = 0L

    // 聚合逻辑 来一条数据就 +1
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    // 获取结果 直接返回accumulator
    override def getResult(accumulator: Long): Long = accumulator

    // 两个累加器做聚合 a + b
    override def merge(a: Long, b: Long): Long = a + b
  }

  // TODO 全窗口聚合函数逻辑编写  用于输出窗口的结果
  // 第一个参数 输入是累加器输出过来的所以是Long
  // 第二个参数 输出 ItemViewCount
  // 第三个参数 Key值 使用了itemId分流的所以是Long
  // 第四个参数 TimeWindow
  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow]{

    // 当窗口闭合的时候调用
    override def process(key: Long,
                         context: Context,
                         // elements里只有1条数据 因为从增量聚合函数过来的所以只有1条数据
                         elements: Iterable[Long],
                         out: Collector[ItemViewCount]): Unit = {
      // 第一个字段是key 第一个字段窗口结束时间  第一个字段浏览量(增量聚合函数发送过来的结果)
      out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
    }
  }
  // TODO 计算最热门TopN商品
  // 因为处理的是keyBy以后的流所以用 KeyedProcessFunction
  // 第一个参数是key 使用窗口结束时间分流的windowEnd是Long类型
  // 第二个参数 输入 ItemViewCount
  // 第三个参数 输出 打印一下每个窗口的topN是什么 String
  class TopN(val n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

    // 惰性赋值一个状态变量 初始化一个列表变量 使用lazy val方式 使用open方式也可以
    lazy val itemListState = getRuntimeContext.getListState(
      // 泛型是输入样例类 起个名字 item-list Types.of明确告诉泛型类型是ItemViewCount这样就不需要flink做类型推导
      new ListStateDescriptor[ItemViewCount]("itemlist-state", Types.of[ItemViewCount])
    )

    // 来一条数据都会调用一次
    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemListState.add(value)  //把来的数据写到状态变量里
      // 注册一个事件时间定时器 什么时候触发促发排序操作呢？当水位线没过窗口结束时间+100ms的时候
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    // 定时器事件  排序逻辑
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      // 把所有Items拿出来存到SCala集合ListBuffer数据类型里面去
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()

      // 导入一些隐式类型转换
      import scala.collection.JavaConversions._

      for(item <- itemListState.get) {
        allItems += item
      }

      itemListState.clear() // 清空状态变量，释放空间 gc

      // 放到内存里进行排序
      val sortedItems = allItems
        .sortBy(-_.count) // 降序排列
        .take(n)

      val result = new StringBuilder
      result
        .append("================================================")
        .append("时间：")
        // 把时间转成能看懂的时间戳 -100 是因为定时器设置时+100 就是窗口结束时间了
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      // 遍历取出来的数据
      for(i <- sortedItems.indices) { // indices遍历的是索引
        val cur = sortedItems(i) // 当前第一名商品
        result
          .append("No")
          .append(i + 1)
          .append("：商品ID = ")
          .append(cur.itemId)
          .append(" 浏览量 = ")
          .append(cur.count)
          .append("\n")
      }

      result
        .append("=============================================================\n\n\n")

      // 为了打印不那么频繁 隔1s打印一次
      Thread.sleep(1000)
      // 隔1s 向下发送数据
      out.collect(result.toString())
    }
  }
}

package com.zwc.project

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
 * TODO 恶意登陆实现
 * 不使用CEP实现 5s内连续两次登陆失败
 */
object LoginFailWithoutCEP {

  case class LoginEvent(userId: String,
                        ipAddr: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
        LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      // 分配升序时间戳
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId) // 按照userId进行分流
      .process(new MatchFunction)

    stream.print()

    env.execute()
  }

  // 实现CEP功能，连续两次失败登陆 报警
  // 泛型第一个参数是key 是userId
  // 泛型第二个参数是输入 是Event
  // 泛型第三个参数是输出 是报警信息
  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, String]{
    // 惰性赋值两个状态变量

    // 存储登陆事件的列表状态
    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-state", Types.of[LoginEvent])
    )

    // 用来存储定时器时间戳状态变量
    lazy val tsState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts-state", Types.of[Long])
    )
    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[String, LoginEvent, String]#Context,
                                out: Collector[String]): Unit = {
      // 操作状态变量检测恶意登陆
      if(value.eventType.equals("fail")) {
        loginState.add(value) // 用来保存登陆失败的事件
        if(tsState.value() == 0L) { // 检查一下有没有定时器存在  // 如果时间戳不存在
          // 5s中之内连续2次登陆失败，报警
          // 注册一个事件时间定时器
          ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
          // 更新状态变量
          tsState.update(value.eventTime.toLong * 1000 + 5000L)
        }
      }

      if(value.eventType.equals("success")){
        loginState.clear()
        ctx.timerService().deleteEventTimeTimer(tsState.value())
        tsState.clear()
      }
    }

    // 定时器
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val allLogins = ListBuffer[LoginEvent]()
      import scala.collection.JavaConversions._
      for(login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()
      if(allLogins.length >= 2) {
        out.collect("5s之内连续两次登录失败")
      }
    }
  }
}

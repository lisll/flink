package com.dinglicom.flink.stream.jdbc

import org.apache.flink.streaming.api.scala._
import com.dingli.flink.stream.jdbc.JdbcReader
import com.dingli.flink.stream.jdbc.JdbcWriter
/*
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.TimeCharacteristic
*/

object TestJdbc {

  def main(args: Array[String]): Unit = {

    //scala代码
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.enableCheckpointing(5000)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    
    val dataStream = env.addSource(new JdbcReader()) //，读取mysql数据，获取dataStream后可以做逻辑处理，这里没有做
    dataStream.addSink(new JdbcWriter()) //写入mysql
    
    env.execute("flink mysql demo") //运行程序
  }
}
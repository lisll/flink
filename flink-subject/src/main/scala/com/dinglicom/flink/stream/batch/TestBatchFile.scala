package com.dinglicom.flink.stream.batch

import com.dingli.flink.stream.sink.MySQLSinkLocal

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichMapFunction

object TestBatchFile {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

	    
    // get input data
    //val text1 = env.readTextFile("F:\\Flink_learn\\src_data\\lteu1_http_201812251259_25_npssig132_8149800117368576.DAT.gz")
    val text2 = env.readTextFile("F:\\Flink_learn\\src_data\\lteu1_http_201812251259_25_npssig132_8150200127858432.DAT.gz")

    //text.setParallelism(2).writeAsText("F:\\Flink_learn\\output2", WriteMode.OVERWRITE)
    /*
    val outData = text1.map(new RichMapFunction[String, Tuple2[String, String]] {
      def map(in: String): Tuple2[String, String] = {
        val line = in.split("\\|")
        val citycode = line(0)
        val msisdn = line(3)
        new Tuple2[String, String](citycode, msisdn)
      }
    })//.keyBy(0, 1)
    
    outData.writeAsCsv("F:\\Flink_learn\\output5", WriteMode.OVERWRITE, "\n", "|") //setParallelism(4)
    */                              
    val outStream = text2.map(new RichMapFunction[String, Tuple2[String, Long]] {
      def map(in: String): Tuple2[String, Long] = {
        val line = in.split("\\|")
        val citycode = line(0)
        val msisdn = line(3)
        val ultraffic = line(34).toLong
        val dltraffic = line(35).toLong
        val totalTraffic = ultraffic + dltraffic
        new Tuple2[String, Long](msisdn, totalTraffic)
      }
    })//.keyBy(0).sum(1)
    //.keyBy(0).timeWindow(Time.minutes(5), Time.minutes(1)).sum(1);
    
    outStream.addSink(new MysqlDBSink[Tuple2]())
    
    //outStream.writeAsCsv("F:\\Flink_learn\\output1", WriteMode.OVERWRITE, "\n", "|") 

    env.execute("test read file")
  }
}
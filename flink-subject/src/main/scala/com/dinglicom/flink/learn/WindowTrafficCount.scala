package com.dinglicom.flink.learn

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.core.fs.FileSystem.WriteMode
import java.lang.Long
import com.dingli.flink.stream.sink.MySQLSink
import org.apache.flink.api.java.tuple.Tuple2
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer
import java.time.ZoneId

object WindowTrafficCount {
  def main(args: Array[String]) {

    //1.create env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // kafka params
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.41.100:9092,172.16.41.178:9092")
    properties.setProperty("group.id", "flumeid_http")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("sasl.kerberos.service.name", "kafka")
    properties.setProperty("security.protocol", "SASL_PLAINTEXT")

    // create a Kafka streaming source consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String]("http514", new SimpleStringSchema(), properties)
    val originalStream: DataStream[String] = env.addSource(kafkaConsumer)

    //Time.minutes(1)|Time.seconds(5)
    //2.
    // business logic
    val resultStream = originalStream.map(x => {
      var line = x.split("\\|")
      var msisdn = line(87) // u64msisdn 
      var ultraffic = line(38) // u32ultraffic
      var dltraffic = line(39) // u32dltraffic      
      var totalTraffic = (ultraffic).toLong  +  (dltraffic).toLong
      new Tuple2[String, Long](msisdn, totalTraffic)
    }).keyBy(0).timeWindow(Time.minutes(1), Time.seconds(30)).sum(1).map(new MapFunction[Tuple2[String,Long], String] {
      override def map(s: Tuple2[String,Long]): String = {
        s.f0 + "," + s.f1
      }
    })

		val hadoopSink = new BucketingSink[String]("hdfs://nameservice1/tmp/test/flink_out5");
		// 使用东八区时间格式"yyyyMMddHH"命名存储区
		hadoopSink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));
		// 下述两种条件满足其一时，创建新的块文件
		// 条件1.设置块大小为50MB
		hadoopSink.setBatchSize(1024 * 1024 * 50);
		// 条件2.设置时间间隔20min
		hadoopSink.setBatchRolloverInterval(5 * 60 * 1000);
		//_part-0-0.pending|_part-0-5.in-progress
		
		// 设置块文件前缀
		hadoopSink.setPendingPrefix("");
		// 设置块文件后缀
		hadoopSink.setPendingSuffix("");
		// 设置运行中的文件前缀
		hadoopSink.setInProgressPrefix(".");
		//part-0-0|part-0-1|.part-0-2.in-progress
		
		resultStream.addSink(hadoopSink)
		
    //不论是本地还是hdfs.若Parallelism>1将把path当成目录名称，若Parallelism=1将把path当成文件名
    //resultStream.writeAsText("hdfs://nameservice1/tmp/test/flink_out2/" + getNowDate(), WriteMode.OVERWRITE);
    
    //resultStream.writeAsCsv("hdfs://nameservice1/tmp/test/flink_out5", WriteMode.OVERWRITE)

    //resultStream.addSink(new MySQLSink())
    
    env.execute("Window Stream SumTraffic")
  }
  
  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm") //yyyyMMddHHmmss
    var nowTime = dateFormat.format(now)
    nowTime
  }  
}

    /*.filter(x => {
      var line = x.split("\\|")
      var citycode = line(91) // u16citycode
      citycode == 25
    })*/
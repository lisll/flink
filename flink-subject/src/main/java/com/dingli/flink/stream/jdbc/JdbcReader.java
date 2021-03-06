package com.dingli.flink.stream.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
@SuppressWarnings("serial")
public class JdbcReader extends RichSourceFunction<Tuple2<String, Long>> {
	
	private String username = "root";
	private String password = "dinglicom";
	private String drivername = "com.mysql.jdbc.Driver";
	private String dburl = "jdbc:mysql://172.16.50.61:3306/flink_db";
	
	private static final Logger logger = LoggerFactory
			.getLogger(JdbcReader.class);

	private Connection connection = null;
	private PreparedStatement ps = null;

	// 该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		Class.forName(drivername);// 加载数据库驱动
		connection = DriverManager.getConnection(dburl, username, password);// 获取连接
		String sql = "select msisdin, traffic from user_traffic_info";
		ps = connection.prepareStatement(sql);
	}

	// 执行查询并获取结果
	@Override
	public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
		try {
			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				String msisdn = resultSet.getString("msisdin");
				Long traffic = resultSet.getLong("traffic");
				logger.error("readJDBC msisdn:{}", msisdn);
				Tuple2<String, Long> tuple2 = new Tuple2<String, Long>();
				tuple2.setFields(msisdn,traffic);
				ctx.collect(tuple2);// 发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
			}
		} catch (Exception e) {
			logger.error("runException:{}", e);
		}

	}

	// 关闭数据库连接
	@Override
	public void cancel() {
		try {
			super.close();
			if (connection != null) {
				connection.close();
			}
			if (ps != null) {
				ps.close();
			}
		} catch (Exception e) {
			logger.error("runException:{}", e);
		}
	}
}

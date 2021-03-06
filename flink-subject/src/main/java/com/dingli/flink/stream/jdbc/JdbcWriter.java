package com.dingli.flink.stream.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 
 */
@SuppressWarnings("serial")
public class JdbcWriter extends RichSinkFunction<Tuple2<String, Long>> {

	private String username = "root";
	private String password = "dinglicom";
	private String drivername = "com.mysql.jdbc.Driver";
	private String dburl = "jdbc:mysql://172.16.50.61:3306/flink_db";

	private Connection connection;
	private PreparedStatement preparedStatement;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// 加载JDBC驱动
		Class.forName(drivername);// 加载数据库驱动
		// 获取数据库连接
		connection = DriverManager.getConnection(dburl, username, password);// 获取连接
		String sql = "replace into user_traffic_info_bak(msisdin,traffic) values(?,?)";
		preparedStatement = connection.prepareStatement(sql);
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}
		super.close();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void invoke(Tuple2<String, Long> value, Context context)
			throws Exception {
		try {
			String msisdn = value.f0;
			Long traffic = value.f1;// 获取JdbcReader发送过来的结果
			preparedStatement.setString(1, msisdn);
			preparedStatement.setLong(2, traffic);			
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

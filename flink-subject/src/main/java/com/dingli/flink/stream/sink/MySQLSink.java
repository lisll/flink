package com.dingli.flink.stream.sink;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MySQLSink extends RichSinkFunction<Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;
	private Connection connection;
	private PreparedStatement preparedStatement;
	private String username = "root";
	private String password = "dinglicom";
	private String drivername = "com.mysql.jdbc.Driver";
	private String dburl = "jdbc:mysql://172.16.50.61:3306/flink_db";
	
	@Override
	public void invoke(Tuple2<String, Long> value) throws Exception {
		try
		{
			if (connection == null) {
				Class.forName(drivername);
				connection = DriverManager.getConnection(dburl, username,
						password);
			}

			String sql = "replace into user_traffic_info(msisdin,traffic) values(?,?)";
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, value.f0);
			preparedStatement.setLong(2, value.f1);
			preparedStatement.executeUpdate();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void open(Configuration parms) throws Exception 
	{
		Class.forName(drivername);
		connection = DriverManager.getConnection(dburl, username, password);
	}

	public void close() throws Exception 
	{
		if (preparedStatement != null) {
			preparedStatement.close();
		}

		if (connection != null) {
			connection.close();
		}

	}
}

package com.sdps.flink.realtime.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;

import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.common.GmallConfig;

public class JdbcUtil {
	// 声明
	private static Connection connection;

	// 初始化连接
	private static Connection init() {
		try {
			Class.forName(GmallConfig.PHOENIX_DRIVER);
			Connection connection = DriverManager
					.getConnection(GmallConfig.PHOENIX_SERVER);
			// 设置连接到的 Phoenix 的库
			connection.setSchema(GmallConfig.HBASE_SCHEMA);
			return connection;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("获取连接失败！");
		}
	}

	public static <T> List<T> queryList(String sql, Class<T> cls) {
		// 初始化连接
		if (connection == null) {
			connection = init();
		}
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			// 编译 SQL
			preparedStatement = connection.prepareStatement(sql);
			// 执行查询
			resultSet = preparedStatement.executeQuery();
			// 获取查询结果中的元数据信息
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			ArrayList<T> list = new ArrayList<>();
			while (resultSet.next()) {
				T t = cls.newInstance();
				for (int i = 1; i < columnCount + 1; i++) {
					BeanUtils.setProperty(t, metaData.getColumnName(i),
							resultSet.getObject(i));
				}
				list.add(t);
			}
			// 返回结果
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("查询维度信息失败！");
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		System.out.println(queryList("select * from DIM_BASE_TRADEMARK",
				JSONObject.class));
	}
}
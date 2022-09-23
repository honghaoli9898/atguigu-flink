package com.sdps.flink.realtime.app.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.common.GmallConfig;
import com.sdps.flink.realtime.util.DimUtil;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
	private static final long serialVersionUID = 1L;
	private Connection connection = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 初始化 Phoenix 连接
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
	}

	// 将数据写入 Phoenix：upsert into t(id,name,sex) values(...,...,...)
	@Override
	public void invoke(JSONObject jsonObject, Context context) throws Exception {
		PreparedStatement preparedStatement = null;
		try {
			// 获取数据中的 Key 以及 Value
			JSONObject data = jsonObject.getJSONObject("after");
			Set<String> keys = data.keySet();
			Collection<Object> values = data.values();
			// 获取表名
			String tableName = jsonObject.getString("sinkTable");
			// 创建插入数据的 SQL
			String upsertSql = genUpsertSql(tableName, keys, values);
			System.out.println(upsertSql);
			// 编译 SQL
			preparedStatement = connection.prepareStatement(upsertSql);
			// 判断如果当前数据为更新,则删除redis数据
			if ("update".equals(jsonObject.getString("type"))) {
				DimUtil.delRedisDimInfo(tableName.toUpperCase(),
						data.getString("id"));
			}
			// 执行
			preparedStatement.executeUpdate();
			// 提交
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("插入 Phoenix 数据失败！");
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	// 创建插入数据的 SQL upsert into t(id,name,sex) values('...','...','...')
	private String genUpsertSql(String tableName, Set<String> keys,
			Collection<Object> values) {
		return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName
				+ "(" + StringUtils.join(keys, ",") + ")" + " values('"
				+ StringUtils.join(values, "','") + "')";
	}
}
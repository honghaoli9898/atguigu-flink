package com.sdps.flink.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.mysql.cj.result.Row;

public class FlinkCDCWithSQL {

	public static void main(String[] args) throws Exception {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStateBackend(new HashMapStateBackend());
		env.getCheckpointConfig().setCheckpointStorage(
				"hdfs://master:8020/flink/ck");
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointingMode(
				CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(10000L);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 开启ck
		tableEnv.executeSql("CREATE TABLE user_info (" + " id INT,"
				+ " name STRING," + " phone_num STRING" + ") WITH ("
				+ " 'connector' = 'mysql-cdc'," + " 'hostname' = 'hadoop102',"
				+ " 'port' = '3306'," + " 'username' = 'root',"
				+ " 'password' = '123456'," + " 'database-name' = 'test',"
				+ " 'table-name' = 'test_cdc'" + ")");
		Table table = tableEnv.sqlQuery("select * from user_info");
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv
				.toRetractStream(table, Row.class);
		retractStream.print();
		env.execute("FlinkCDCWithSQL");
	}

}

package com.sdps.flink.cdc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

public class FlinkCDC {
	public static void main(String[] args) throws Exception {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		// 开启ck
//		env.setStateBackend(new FsStateBackend("hdfs://master:8020/flink/ck"));
//		env.enableCheckpointing(5000);
//		env.getCheckpointConfig().setCheckpointingMode(
//				CheckpointingMode.EXACTLY_ONCE);
//		env.getCheckpointConfig().setCheckpointTimeout(10000L);
//		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
//		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));

		// 2.通过flinkcdc构建source
		DebeziumSourceFunction<String> sourceFunction = MySQLSource
				.<String> builder().hostname("10.1.3.25").port(3306)
				.username("root").password("123456").databaseList("test")
				.tableList("test.test_cdc")
				.deserializer(new StringDebeziumDeserializationSchema())
				.startupOptions(StartupOptions.initial()).build();
		DataStreamSource<String> streamSource = env.addSource(sourceFunction);
		streamSource.print();
		env.execute("flink-cdc");
	}
}

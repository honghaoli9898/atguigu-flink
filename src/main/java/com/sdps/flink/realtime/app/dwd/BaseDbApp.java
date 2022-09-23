package com.sdps.flink.realtime.app.dwd;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.sdps.flink.cdc.CustomerDeserilzation;
import com.sdps.flink.realtime.app.function.DimSinkFunction;
import com.sdps.flink.realtime.app.function.TableProcessFunction;
import com.sdps.flink.realtime.bean.TableProcess;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class BaseDbApp {

	public static void main(String[] args) throws Exception {
		// 1.执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		// 开启ck
		env.setStateBackend(new HashMapStateBackend());
		// 将checkpoint数据保存在外部的HDFS中
		env.getCheckpointConfig().setCheckpointStorage(
				"hdfs://master:8020/flink/ck");
		env.getCheckpointConfig().setCheckpointingMode(
				CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(10000L);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
		// System.setProperty("HADOOP_USER_NAME", "hdfs");

		// 2.读取 Kafka 数据
		String topic = "ods_base_db";
		String groupId = "ods_db_group";
		FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(
				topic, groupId);
		DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
		// 3.将每行数据转换为 JSON 对象
		SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
				.map(JSON::parseObject);
		// 4.过滤
		SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS
				.filter(new FilterFunction<JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(JSONObject value) throws Exception {
						// 获取 data 字段
						String type = value.getString("type");
						return !"delete".equals(type);
					}
				});
		// 5.创建 MySQL CDC Source
		DebeziumSourceFunction<String> sourceFunction = MySQLSource
				.<String> builder().hostname("hadoop102").port(3306)
				.username("root").password("000000")
				.databaseList("gmall2021-realtime")
				.tableList("gmall2021-realtime.table_process")
				.startupOptions(StartupOptions.initial())
				.deserializer(new CustomerDeserilzation()).build();
		// 6.读取 MySQL 数据
		DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
		// 7.将配置信息流作为广播流
		MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
				"table-process-state", String.class, TableProcess.class);
		BroadcastStream<String> broadcastStream = tableProcessDS
				.broadcast(mapStateDescriptor);
		// 8.将主流和广播流进行链接
		BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS
				.connect(broadcastStream);
		OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag");
		SingleOutputStreamOperator<JSONObject> kafkaJsonDS = connectedStream
				.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
		DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS
				.getSideOutput(hbaseTag);
		// 7.执行任务
		hbaseJsonDS.addSink(new DimSinkFunction());
		FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil
				.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void open(InitializationContext context)
							throws Exception {
						System.out.println("开始序列化 Kafka 数据！");
					}

					@Override
					public ProducerRecord<byte[], byte[]> serialize(
							JSONObject element, @Nullable Long timestamp) {
						return new ProducerRecord<byte[], byte[]>(element
								.getString("sinkTable"), element.getString(
								"after").getBytes());
					}
				});
		kafkaJsonDS.addSink(kafkaSinkBySchema);
		env.execute();
	}
}
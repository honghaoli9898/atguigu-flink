package com.sdps.flink.realtime.app.dwm;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class UniqueVisitApp {
	public static void main(String[] args) throws Exception {
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

		// 2.读取 Kafka dwd_page_log 主题数据创建流
		String groupId = "unique_visit_app";
		String sourceTopic = "dwd_page_log";
		String sinkTopic = "dwm_unique_visit";
		FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(
				sourceTopic, groupId);
		DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
		// 3.将每行数据转换为 JSON 对象
		SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
				.map(JSON::parseObject);
		// 4.按照 mid 分组
		KeyedStream<JSONObject, String> keyedStream = jsonObjDS
				.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString(
						"mid"));
		SingleOutputStreamOperator<JSONObject> uvDs = keyedStream
				.filter(new RichFilterFunction<JSONObject>() {

					private static final long serialVersionUID = 1L;
					private ValueState<String> dateState;

					@Override
					public void open(Configuration configuration)
							throws Exception {
						ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>(
								"date-state", String.class);
						StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(
								Time.hours(24)).setUpdateType(
								UpdateType.OnCreateAndWrite).build();
						valueStateDescriptor.enableTimeToLive(stateTtlConfig);
						dateState = getRuntimeContext().getState(
								valueStateDescriptor);

					}

					@Override
					public boolean filter(JSONObject value) throws Exception {
						String lastPageId = value.getJSONObject("page")
								.getString("late_page_id");
						if (StrUtil.isNotBlank(lastPageId)) {
							String lastDate = dateState.value();
							String curDate = DateUtil.date().toString();
							if (!curDate.equals(lastDate)) {
								dateState.update(curDate);
								return true;
							}
						}
						return false;
					}
				});
		jsonObjDS.print();
		uvDs.map(JSONAware::toJSONString).addSink(
				MyKafkaUtil.getKafkaSink(sinkTopic));
		// 7.启动任务
		env.execute();
	}
}

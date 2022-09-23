package com.sdps.flink.realtime.app.dwm;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import cn.hutool.core.util.StrUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class UserJumpDetailApp {
	public static void main(String[] args) throws Exception {
		// 1.执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);
		// env.setStateBackend(new HashMapStateBackend());
		// env.getCheckpointConfig().setCheckpointStorage(
		// "hdfs://master:8020/flink/ck");
		// env.getCheckpointConfig().setCheckpointingMode(
		// CheckpointingMode.EXACTLY_ONCE);
		// env.getCheckpointConfig().setCheckpointTimeout(10000L);
		// env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		// env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
		// env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
		String sourceTopic = "dwd_page_log";
		String groupId = "userJumpDetailApp";
		String sinkTopic = "dwm_user_jump_detail";
		FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(
				sourceTopic, groupId);
		DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
		// DataStream<String> kafkaDS = env.socketTextStream("hadoop102", 9999);
		// 提取数据中的时间戳生成 Watermark
		// 老版本,默认使用的处理时间语义,新版本默认时间语义为事件时间
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 3.将数据转换为 JSON 对象
		SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
				JSON::parseObject).assignTimestampsAndWatermarks(
				WatermarkStrategy.<JSONObject> forBoundedOutOfOrderness(
						Duration.ofSeconds(2)).withTimestampAssigner(
						new SerializableTimestampAssigner<JSONObject>() {

							private static final long serialVersionUID = 1L;

							@Override
							public long extractTimestamp(JSONObject element,
									long recordTimestamp) {
								return element.getLong("ts");
							}
						}));
		Pattern<JSONObject, JSONObject> pattern = Pattern
				.<JSONObject> begin("start")
				.where(new SimpleCondition<JSONObject>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(JSONObject value) throws Exception {
						String lastPageId = value.getJSONObject("page")
								.getString("last_page_id");
						return StrUtil.isBlank(lastPageId);
					}
				}).next("next").where(new SimpleCondition<JSONObject>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(JSONObject value) throws Exception {
						String lastPageId = value.getJSONObject("page")
								.getString("last_page_id");
						return StrUtil.isBlank(lastPageId);
					}
				}).within(Time.seconds(10));

		// cep循环模式
		Pattern.<JSONObject> begin("start")
				.where(new SimpleCondition<JSONObject>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(JSONObject value) throws Exception {
						String lastPageId = value.getJSONObject("page")
								.getString("last_page_id");
						return StrUtil.isBlank(lastPageId);
					}
				}).times(2).consecutive() // 严格近邻
				.within(Time.seconds(10));
		PatternStream<JSONObject> patternStream = CEP.pattern(
				jsonObjDS.keyBy(json -> json.getJSONObject("common").getString(
						"mid")), pattern);
		OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeout");
		SingleOutputStreamOperator<JSONObject> selectDs = patternStream.select(
				timeOutTag,
				new PatternTimeoutFunction<JSONObject, JSONObject>() {

					private static final long serialVersionUID = 1L;

					@Override
					public JSONObject timeout(
							Map<String, List<JSONObject>> map, long arg1)
							throws Exception {
						return map.get("start").get(0);
					}
				}, new PatternSelectFunction<JSONObject, JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
					public JSONObject select(Map<String, List<JSONObject>> map)
							throws Exception {
						return map.get("start").get(0);
					}
				});
		DataStream<JSONObject> timeOutDs = selectDs.getSideOutput(timeOutTag);
		DataStream<JSONObject> unionDs = selectDs.union(timeOutDs);
		unionDs.map(JSONAware::toJSONString).addSink(
				MyKafkaUtil.getKafkaSink(sinkTopic));
		env.execute("UserJumpDetailApp");
	}
}

package com.sdps.flink.realtime.app.dwd;

import java.util.Objects;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class BaseLogApp {

	public static void main(String[] args) throws Exception {
		// 1.执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		// 开启ck
		// env.setStateBackend(new
		// FsStateBackend("hdfs://master:8020/flink/ck"));
		// env.enableCheckpointing(5000);
		// env.getCheckpointConfig().setCheckpointingMode(
		// CheckpointingMode.EXACTLY_ONCE);
		// env.getCheckpointConfig().setCheckpointTimeout(10000L);
		// env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		// env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
		// env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
		// System.setProperty("HADOOP_USER_NAME", "hdfs");

		// 2.消费ods_base_log
		String sourceTopic = "ods_base_log";
		String groupId = "base_log_app_20220129";
		DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil
				.getKafkaConsumer(sourceTopic, groupId));
		// 3.转换json
		OutputTag<String> outputTag = new OutputTag<String>("Dirty");
		SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
				.process(new ProcessFunction<String, JSONObject>() {

					private static final long serialVersionUID = -1610086910935589358L;

					@Override
					public void processElement(String value, Context context,
							Collector<JSONObject> out) throws Exception {
						if (JSONUtil.isJson(value)) {
							out.collect(JSONObject.parseObject(value));
						} else {
							context.output(outputTag, value);
						}
					}
				});
		// 4.新老用户校验
		SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS
				.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString(
						"mid")).map(
						new RichMapFunction<JSONObject, JSONObject>() {

							private static final long serialVersionUID = 4742994325588707015L;
							private ValueState<String> valueState;

							@Override
							public void open(Configuration parameters)
									throws Exception {
								valueState = getRuntimeContext().getState(
										new ValueStateDescriptor<String>(
												"value-state", String.class));
							};

							@Override
							public JSONObject map(JSONObject value)
									throws Exception {
								String isNew = value.getString("is_new");
								if (Objects.equals("1", isNew)) {
									String state = valueState.value();
									if (Objects.nonNull(state)) {
										value.getJSONObject("common").put(
												"is_new", 0);
									} else {
										valueState.update("1");
									}
								}
								return value;

							}
						});
		// 5.分流
		OutputTag<String> startOutputTag = new OutputTag<String>("start");
		OutputTag<String> displayOutputTag = new OutputTag<String>("display");
		SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS
				.process(new ProcessFunction<JSONObject, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(JSONObject value, Context ctx,
							Collector<String> out) throws Exception {
						String start = value.getString("start");
						if (StrUtil.isNotBlank(start)) {
							ctx.output(startOutputTag, value.toJSONString());
						} else {
							out.collect(value.toJSONString());
							JSONArray displays = value.getJSONArray("displays");
							if (CollUtil.isNotEmpty(displays)) {
								String pageId = value.getJSONObject("page")
										.getString("page_id");
								displays.forEach(display -> {
									JSONObject jsonObj = (JSONObject) display;
									jsonObj.put("page_id", pageId);
									ctx.output(displayOutputTag,
											jsonObj.toJSONString());
								});
							}
						}
					}
				});
		// 6.提取侧输出流
		DataStream<String> startDs = pageDS.getSideOutput(startOutputTag);
		DataStream<String> displayDs = pageDS.getSideOutput(displayOutputTag);
		startDs.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
		displayDs.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
		pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
		env.execute("BaseLogApp");
	}
}

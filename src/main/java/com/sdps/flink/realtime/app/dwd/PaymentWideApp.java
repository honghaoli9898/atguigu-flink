package com.sdps.flink.realtime.app.dwd;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import cn.hutool.core.date.DateUtil;

import com.alibaba.fastjson.JSON;
import com.sdps.flink.realtime.bean.OrderWide;
import com.sdps.flink.realtime.bean.PaymentInfo;
import com.sdps.flink.realtime.bean.PaymentWide;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class PaymentWideApp {

	public static void main(String[] args) throws Exception {
		// 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);
		// 1.1 设置状态后端
		env.setStateBackend(new HashMapStateBackend());
		// 将checkpoint数据保存在外部的HDFS中
		env.getCheckpointConfig().setCheckpointStorage(
				"hdfs://master:8020/flink/ck");
		// 1.2 开启 CK
		// env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
		// env.getCheckpointConfig().setCheckpointTimeout(60000L);
		// 修改用户名
		// System.setProperty("HADOOP_USER_NAME", "atguigu");
		// 2.读取 Kafka 主题数据 dwd_payment_info dwm_order_wide
		String groupId = "payment_wide_group";
		String paymentInfoSourceTopic = "dwd_payment_info";
		String orderWideSourceTopic = "dwm_order_wide";
		String paymentWideSinkTopic = "dwm_payment_wide";
		DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil
				.getKafkaConsumer(paymentInfoSourceTopic, groupId));
		DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil
				.getKafkaConsumer(orderWideSourceTopic, groupId));
		// 3.将数据转换为 JavaBean 并提取时间戳生成 WaterMark
		SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS
				.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<PaymentInfo> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<PaymentInfo>() {
											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													PaymentInfo element,
													long recordTimestamp) {

												return DateUtil
														.parse(element
																.getCreate_time())
														.getTime();
											}
										}));
		SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS
				.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<OrderWide> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<OrderWide>() {

											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													OrderWide element,
													long recordTimestamp) {
												return DateUtil
														.parse(element
																.getCreate_time())
														.getTime();
											}
										}));
		// 4.按照 OrderID 分组
		KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = paymentInfoDS
				.keyBy(PaymentInfo::getOrder_id);
		KeyedStream<OrderWide, Long> orderWideKeyedStream = orderWideDS
				.keyBy(OrderWide::getOrder_id);
		// 5.双流 JOIN
		SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream
				.intervalJoin(orderWideKeyedStream)
				.between(Time.minutes(-15), Time.seconds(0))
				.process(
						new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(PaymentInfo paymentInfo,
									OrderWide orderWide, Context ctx,
									Collector<PaymentWide> out)
									throws Exception {
								out.collect(new PaymentWide(paymentInfo,
										orderWide));
							}
						});
		// 打印测试
		paymentWideDS.print(">>>>>>>>>>");
		// 6.将数据写入 Kafka dwm_payment_wide

		paymentWideDS.map(JSON::toJSONString).addSink(
				MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
		// 7.启动任务
		env.execute("PaymentWideApp");
	}
}

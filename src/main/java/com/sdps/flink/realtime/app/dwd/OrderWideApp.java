package com.sdps.flink.realtime.app.dwd;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import cn.hutool.core.date.DateUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.app.function.DimAsyncFunction;
import com.sdps.flink.realtime.bean.OrderDetail;
import com.sdps.flink.realtime.bean.OrderInfo;
import com.sdps.flink.realtime.bean.OrderWide;
import com.sdps.flink.realtime.util.MyKafkaUtil;

public class OrderWideApp {

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

		// //1.2 开启 CK
		// env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
		// env.getCheckpointConfig().setCheckpointTimeout(60000L);
		// 2.读取 Kafka 订单和订单明细主题数据 dwd_order_info dwd_order_detail
		String orderInfoSourceTopic = "dwd_order_info";
		String orderDetailSourceTopic = "dwd_order_detail";
		String orderWideSinkTopic = "dwm_order_wide";
		String groupId = "order_wide_group";
		FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil
				.getKafkaConsumer(orderInfoSourceTopic, groupId);
		SingleOutputStreamOperator<OrderInfo> orderInfoKafkaDS = env
				.addSource(orderInfoKafkaSource)
				.map(line -> {
					OrderInfo orderInfo = JSON.parseObject(line,
							OrderInfo.class);
					String create_time = orderInfo.getCreate_time();
					String[] dateTimeArr = create_time.split(" ");
					orderInfo.setCreate_date(dateTimeArr[0]);
					orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
					orderInfo.setCreate_ts(DateUtil
							.parse("yyyy-MM-dd HH:mm:ss").getTime());
					return orderInfo;
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<OrderInfo> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<OrderInfo>() {
											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													OrderInfo element,
													long recordTimestamp) {
												return element.getCreate_ts();
											}
										}));
		FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil
				.getKafkaConsumer(orderDetailSourceTopic, groupId);
		SingleOutputStreamOperator<OrderDetail> orderDetailKafkaDS = env
				.addSource(orderDetailKafkaSource)
				.map(line -> {
					OrderDetail orderDetail = JSON.parseObject(line,
							OrderDetail.class);
//					String create_time = orderDetail.getCreate_time();
					orderDetail.setCreate_ts(DateUtil.parse(
							"yyyy-MM-dd HH:mm:ss").getTime());
					return orderDetail;
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<OrderDetail> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<OrderDetail>() {
											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													OrderDetail element,
													long recordTimestamp) {
												return element.getCreate_ts();
											}
										}));
		SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDs = orderInfoKafkaDS
				.keyBy(OrderInfo::getId)
				.intervalJoin(
						orderDetailKafkaDS.keyBy(OrderDetail::getOrder_id))
				.between(Time.seconds(-5), Time.seconds(5))
				.process(
						new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(OrderInfo left,
									OrderDetail right, Context context,
									Collector<OrderWide> out) throws Exception {
								out.collect(new OrderWide(left, right));
							}
						});
		// 关联维度信息
		// SingleOutputStreamOperator<OrderWide> orderWideWithDimDs =
		// orderWideWithNoDimDs
		// .map(
		// orderWide -> {
		// // 关联用户维度
		// Long userId = orderWide.getUser_id();
		//
		// return orderWide;
		// });
		SingleOutputStreamOperator<OrderWide> orderWideWithUserDs = AsyncDataStream
				.unorderedWait(orderWideWithNoDimDs,
						new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
							private static final long serialVersionUID = 1L;

							@Override
							public String getKey(OrderWide input) {
								return input.getUser_id().toString();
							}

							@Override
							public void join(OrderWide input, JSONObject dimInfo)
									throws Exception {
								input.setUser_gender(dimInfo
										.getString("GENDER"));
								String birthday = dimInfo.getString("BRITHDAY");
								long ts = DateUtil.parse(birthday).getTime();
								long currentTs = System.currentTimeMillis();
								Long age = (currentTs - ts)
										/ (1000 * 60 * 60 * 24 * 365L);
								input.setUser_age(age.intValue());
							}
						}, 60L, TimeUnit.SECONDS);
		SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream
				.unorderedWait(orderWideWithUserDs,
						new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
							private static final long serialVersionUID = 1L;

							@Override
							public String getKey(OrderWide input) {
								return input.getProvince_id().toString();
							}

							@Override
							public void join(OrderWide input, JSONObject dimInfo)
									throws Exception {
								input.setProvince_3166_2_code(dimInfo
										.getString("ISO_3166_2"));
								input.setProvince_area_code(dimInfo
										.getString("AREA_CODE"));
								input.setProvince_name(dimInfo
										.getString("NAME"));
								input.setProvince_iso_code(dimInfo
										.getString("ISO_CODE"));
							}
						}, 60L, TimeUnit.SECONDS);
		SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream
				.unorderedWait(orderWideWithProvinceDS,
						new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(OrderWide orderWide,
									JSONObject jsonObject) throws Exception {
								orderWide.setSku_name(jsonObject
										.getString("SKU_NAME"));
								orderWide.setCategory3_id(jsonObject
										.getLong("CATEGORY3_ID"));
								orderWide.setSpu_id(jsonObject
										.getLong("SPU_ID"));
								orderWide.setTm_id(jsonObject.getLong("TM_ID"));
							}

							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getSku_id());
							}
						}, 60, TimeUnit.SECONDS);
		SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream
				.unorderedWait(orderWideWithSkuDS,
						new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(OrderWide orderWide,
									JSONObject jsonObject) throws Exception {
								orderWide.setSpu_name(jsonObject
										.getString("SPU_NAME"));
							}

							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getSpu_id());
							}
						}, 60, TimeUnit.SECONDS);
		// 5.5 关联品牌维度
		SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream
				.unorderedWait(orderWideWithSpuDS,
						new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(OrderWide orderWide,
									JSONObject jsonObject) throws Exception {
								orderWide.setTm_name(jsonObject
										.getString("TM_NAME"));
							}

							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getTm_id());
							}
						}, 60, TimeUnit.SECONDS);
		// 5.6 关联品类维度
		SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream
				.unorderedWait(orderWideWithTmDS,
						new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(OrderWide orderWide,
									JSONObject jsonObject) throws Exception {
								orderWide.setCategory3_name(jsonObject
										.getString("NAME"));
							}

							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide
										.getCategory3_id());
							}
						}, 60, TimeUnit.SECONDS);
		orderWideWithCategory3DS.map(JSON::toJSONString).addSink(
				MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
		env.execute("OrderWideApp");
	}
}

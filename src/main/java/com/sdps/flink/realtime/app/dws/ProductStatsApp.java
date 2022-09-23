package com.sdps.flink.realtime.app.dws;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sdps.flink.realtime.app.function.DimAsyncFunction;
import com.sdps.flink.realtime.bean.OrderWide;
import com.sdps.flink.realtime.bean.PaymentWide;
import com.sdps.flink.realtime.bean.ProductStats;
import com.sdps.flink.realtime.common.GmallConstant;
import com.sdps.flink.realtime.util.ClickHouseUtil;
import com.sdps.flink.realtime.util.DateTimeUtil;
import com.sdps.flink.realtime.util.MyKafkaUtil;

/**
 * Desc: 形成以商品为准的统计 曝光 点击 购物车 下单 支付 退单 评论数 宽表
 */
public class ProductStatsApp {
	public static void main(String[] args) throws Exception {
		// TODO 0.基本环境准备
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		// 设置并行度
		env.setParallelism(4);
		/*
		 * //检查点 CK 相关设置 env.enableCheckpointing(5000,
		 * CheckpointingMode.AT_LEAST_ONCE);
		 * env.getCheckpointConfig().setCheckpointTimeout(60000); StateBackend
		 * fsStateBackend = new FsStateBackend(
		 * "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
		 * env.setStateBackend(fsStateBackend);
		 * System.setProperty("HADOOP_USER_NAME","atguigu");
		 */
		// TODO 1.从 Kafka 中获取数据流
		String groupId = "product_stats_app";
		String pageViewSourceTopic = "dwd_page_log";
		String orderWideSourceTopic = "dwm_order_wide";
		String paymentWideSourceTopic = "dwm_payment_wide";
		String cartInfoSourceTopic = "dwd_cart_info";
		String favorInfoSourceTopic = "dwd_favor_info";
		String refundInfoSourceTopic = "dwd_order_refund_info";
		String commentInfoSourceTopic = "dwd_comment_info";
		FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil
				.getKafkaConsumer(pageViewSourceTopic, groupId);
		FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil
				.getKafkaConsumer(orderWideSourceTopic, groupId);
		FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil
				.getKafkaConsumer(paymentWideSourceTopic, groupId);
		FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil
				.getKafkaConsumer(favorInfoSourceTopic, groupId);
		FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil
				.getKafkaConsumer(cartInfoSourceTopic, groupId);
		FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil
				.getKafkaConsumer(refundInfoSourceTopic, groupId);
		FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil
				.getKafkaConsumer(commentInfoSourceTopic, groupId);
		DataStreamSource<String> pageViewDStream = env
				.addSource(pageViewSource);
		DataStreamSource<String> favorInfoDStream = env
				.addSource(favorInfoSourceSouce);
		DataStreamSource<String> orderWideDStream = env
				.addSource(orderWideSource);
		DataStreamSource<String> paymentWideDStream = env
				.addSource(paymentWideSource);
		DataStreamSource<String> cartInfoDStream = env
				.addSource(cartInfoSource);
		DataStreamSource<String> refundInfoDStream = env
				.addSource(refundInfoSource);
		DataStreamSource<String> commentInfoDStream = env
				.addSource(commentInfoSource);

		// TODO 2.对获取的流数据进行结构的转换
		// 2.1 转换曝光及页面流数据
		SingleOutputStreamOperator<ProductStats> pageAndDispStatsDstream = pageViewDStream
				.process(new ProcessFunction<String, ProductStats>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(String json, Context ctx,
							Collector<ProductStats> out) throws Exception {
						JSONObject jsonObj = JSON.parseObject(json);
						JSONObject pageJsonObj = jsonObj.getJSONObject("page");
						String pageId = pageJsonObj.getString("page_id");
						Long ts = jsonObj.getLong("ts");
						if (pageId.equals("good_detail")
								&& "sku_id".equals(pageJsonObj
										.getString("item_type"))) {
							Long skuId = pageJsonObj.getLong("item");
							ProductStats productStats = ProductStats.builder()
									.sku_id(skuId).click_ct(1L).ts(ts).build();
							out.collect(productStats);
						}
						JSONArray displays = jsonObj.getJSONArray("displays");
						if (displays != null && displays.size() > 0) {
							for (int i = 0; i < displays.size(); i++) {
								JSONObject display = displays.getJSONObject(i);
								if (display.getString("item_type").equals(
										"sku_id")) {
									Long skuId = display.getLong("item");
									ProductStats productStats = ProductStats
											.builder().sku_id(skuId)
											.display_ct(1L).ts(ts).build();
									out.collect(productStats);
								}
							}
						}
					}
				});
		// 2.2 转换下单流数据
		SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream
				.map(json -> {
					OrderWide orderWide = JSON.parseObject(json,
							OrderWide.class);
					String create_time = orderWide.getCreate_time();
					Long ts = DateTimeUtil.toTs(create_time);
					return ProductStats
							.builder()
							.sku_id(orderWide.getSku_id())
							.orderIdSet(
									new HashSet<Long>(Collections
											.singleton(orderWide.getOrder_id())))
							.order_sku_num(orderWide.getSku_num())
							.order_amount(orderWide.getSplit_total_amount())
							.ts(ts).build();
				});
		// 2.3 转换收藏流数据
		SingleOutputStreamOperator<ProductStats> favorStatsDstream = favorInfoDStream
				.map(json -> {
					JSONObject favorInfo = JSON.parseObject(json);
					Long ts = DateTimeUtil.toTs(favorInfo
							.getString("create_time"));
					return ProductStats.builder()
							.sku_id(favorInfo.getLong("sku_id")).favor_ct(1L)
							.ts(ts).build();
				});
		// 2.4 转换购物车流数据
		SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream
				.map(json -> {
					JSONObject cartInfo = JSON.parseObject(json);
					Long ts = DateTimeUtil.toTs(cartInfo
							.getString("create_time"));
					return ProductStats.builder()
							.sku_id(cartInfo.getLong("sku_id")).cart_ct(1L)
							.ts(ts).build();
				});
		// 2.5 转换支付流数据
		SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream
				.map(json -> {
					PaymentWide paymentWide = JSON.parseObject(json,
							PaymentWide.class);
					Long ts = DateTimeUtil.toTs(paymentWide
							.getPayment_create_time());
					return ProductStats
							.builder()
							.sku_id(paymentWide.getSku_id())
							.payment_amount(paymentWide.getSplit_total_amount())
							.paidOrderIdSet(
									new HashSet<Long>(Collections
											.singleton(paymentWide
													.getOrder_id()))).ts(ts)
							.build();
				});
		// 2.6 转换退款流数据
		SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream
				.map(json -> {
					JSONObject refundJsonObj = JSON.parseObject(json);
					Long ts = DateTimeUtil.toTs(refundJsonObj
							.getString("create_time"));
					ProductStats productStats = ProductStats
							.builder()
							.sku_id(refundJsonObj.getLong("sku_id"))
							.refund_amount(
									refundJsonObj
											.getBigDecimal("refund_amount"))
							.refundOrderIdSet(
									new HashSet<Long>(Collections
											.singleton(refundJsonObj
													.getLong("order_id"))))
							.ts(ts).build();
					return productStats;
				});
		// 2.7 转换评价流数据
		SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream
				.map(json -> {
					JSONObject commonJsonObj = JSON.parseObject(json);
					Long ts = DateTimeUtil.toTs(commonJsonObj
							.getString("create_time"));
					Long goodCt = GmallConstant.APPRAISE_GOOD
							.equals(commonJsonObj.getString("appraise")) ? 1L
							: 0L;
					ProductStats productStats = ProductStats.builder()
							.sku_id(commonJsonObj.getLong("sku_id"))
							.comment_ct(1L).good_comment_ct(goodCt).ts(ts)
							.build();
					return productStats;
				});
		// TODO 3.把统一的数据结构流合并为一个流
		DataStream<ProductStats> productStatDetailDStream = pageAndDispStatsDstream
				.union(orderWideStatsDstream, cartStatsDstream,
						paymentStatsDstream, refundStatsDstream,
						favorStatsDstream, commonInfoStatsDstream);
		// TODO 4.设定事件时间与水位线
		SingleOutputStreamOperator<ProductStats> productStatsWithTsStream = productStatDetailDStream
				.assignTimestampsAndWatermarks(

				WatermarkStrategy.<ProductStats> forMonotonousTimestamps()
						.withTimestampAssigner(
								(productStats, recordTimestamp) -> {
									return productStats.getTs();
								}));
		// TODO 5.分组、开窗、聚合
		SingleOutputStreamOperator<ProductStats> productStatsDstream = productStatsWithTsStream
		// 5.1 按照商品 id 进行分组
				.keyBy(new KeySelector<ProductStats, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long getKey(ProductStats productStats)
							throws Exception {
						return productStats.getSku_id();
					}
				})
				// 5.2 开窗 窗口长度为 10s
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				// 5.3 对窗口中的数据进行聚合
				.reduce(new ReduceFunction<ProductStats>() {
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("unchecked")
					@Override
					public ProductStats reduce(ProductStats stats1,
							ProductStats stats2) throws Exception {
						stats1.setDisplay_ct(stats1.getDisplay_ct()
								+ stats2.getDisplay_ct());
						stats1.setClick_ct(stats1.getClick_ct()
								+ stats2.getClick_ct());
						stats1.setCart_ct(stats1.getCart_ct()
								+ stats2.getCart_ct());
						stats1.setFavor_ct(stats1.getFavor_ct()
								+ stats2.getFavor_ct());

						stats1.setOrder_amount(stats1.getOrder_amount().add(
								stats2.getOrder_amount()));
						stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
						stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
						stats1.setOrder_sku_num(stats1.getOrder_sku_num()
								+ stats2.getOrder_sku_num());

						stats1.setPayment_amount(stats1.getPayment_amount()
								.add(stats2.getPayment_amount()));
						stats1.getRefundOrderIdSet().addAll(
								stats2.getRefundOrderIdSet());
						stats1.setRefund_order_ct(stats1.getRefundOrderIdSet()
								.size() + 0L);

						stats1.setRefund_amount(stats1.getRefund_amount().add(
								stats2.getRefund_amount()));
						stats1.getPaidOrderIdSet().addAll(
								stats2.getPaidOrderIdSet());
						stats1.setPaid_order_ct(stats1.getPaidOrderIdSet()
								.size() + 0L);
						stats1.setComment_ct(stats1.getComment_ct()
								+ stats2.getComment_ct());
						stats1.setGood_comment_ct(stats1.getGood_comment_ct()
								+ stats2.getGood_comment_ct());
						return stats1;
					}
				},
						new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void apply(
									Long aLong,
									TimeWindow window,
									Iterable<ProductStats> productStatsIterable,
									Collector<ProductStats> out)
									throws Exception {
								SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
										"yyyy-MM-dd HH:mm:ss");
								for (ProductStats productStats : productStatsIterable) {
									productStats.setStt(simpleDateFormat
											.format(window.getStart()));
									productStats.setEdt(simpleDateFormat
											.format(window.getEnd()));
									productStats.setTs(new Date().getTime());
									productStats
											.setOrder_ct((long) productStats
													.getOrderIdSet().size());
									productStats
											.setPaid_order_ct((long) productStats
													.getPaidOrderIdSet().size());
									productStats
											.setRefund_order_ct((long) productStats
													.getRefundOrderIdSet()
													.size());
									out.collect(productStats);
								}
							}
						});
		// TODO 6.补充商品维度信息
		// 6.1 补充 SKU 维度
		SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream = AsyncDataStream
				.unorderedWait(productStatsDstream,
						new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(ProductStats productStats,
									JSONObject jsonObject) throws Exception {
								productStats.setSku_name(jsonObject
										.getString("SKU_NAME"));
								productStats.setSku_price(jsonObject
										.getBigDecimal("PRICE"));
								productStats.setCategory3_id(jsonObject
										.getLong("CATEGORY3_ID"));
								productStats.setSpu_id(jsonObject
										.getLong("SPU_ID"));
								productStats.setTm_id(jsonObject
										.getLong("TM_ID"));
							}

							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getSku_id());
							}
						}, 60, TimeUnit.SECONDS);
		// 6.2 补充 SPU 维度
		SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream = AsyncDataStream
				.unorderedWait(productStatsWithSkuDstream,
						new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(ProductStats productStats,
									JSONObject jsonObject) throws Exception {
								productStats.setSpu_name(jsonObject
										.getString("SPU_NAME"));
							}

							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getSpu_id());
							}
						}, 60, TimeUnit.SECONDS);
		// 6.3 补充品类维度
		SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream = AsyncDataStream
				.unorderedWait(
						productStatsWithSpuDstream,
						new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(ProductStats productStats,
									JSONObject jsonObject) throws Exception {
								productStats.setCategory3_name(jsonObject
										.getString("NAME"));
							}

							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats
										.getCategory3_id());
							}
						}, 60, TimeUnit.SECONDS);
		// 6.4 补充品牌维度
		SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream = AsyncDataStream
				.unorderedWait(
						productStatsWithCategory3Dstream,
						new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
							private static final long serialVersionUID = 1L;

							@Override
							public void join(ProductStats productStats,
									JSONObject jsonObject) throws Exception {
								productStats.setTm_name(jsonObject
										.getString("TM_NAME"));
							}

							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getTm_id());
							}
						}, 60, TimeUnit.SECONDS);
		productStatsWithTmDstream
				.addSink(ClickHouseUtil
						.<ProductStats> getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
		env.execute();
	}
}
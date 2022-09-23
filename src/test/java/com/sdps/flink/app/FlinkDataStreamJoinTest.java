package com.sdps.flink.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.sdps.flink.bean.Bean1;
import com.sdps.flink.bean.Bean2;

public class FlinkDataStreamJoinTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		SingleOutputStreamOperator<Bean1> stream1 = env
				.socketTextStream("master", 8888)
				.map(line -> {
					String[] fields = line.split(",");
					return new Bean1(fields[0], fields[1], Long
							.parseLong(fields[2]));
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Bean1> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<Bean1>() {
											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													Bean1 element,
													long recordTimestamp) {
												return element.getTs() * 1000L;
											}
										}));

		SingleOutputStreamOperator<Bean2> stream2 = env
				.socketTextStream("master", 9999)
				.map(line -> {
					String[] fields = line.split(",");
					return new Bean2(fields[0], fields[1], Long
							.parseLong(fields[2]));
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<Bean2> forMonotonousTimestamps()
								.withTimestampAssigner(
										new SerializableTimestampAssigner<Bean2>() {
											private static final long serialVersionUID = 1L;

											@Override
											public long extractTimestamp(
													Bean2 element,
													long recordTimestamp) {
												return element.getTs() * 1000L;
											}
										}));
		SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDs = stream1
				.keyBy(Bean1::getId)
				.intervalJoin(stream2.keyBy(Bean2::getId))
				.between(Time.seconds(-5), Time.seconds(5))
				.lowerBoundExclusive()
				.upperBoundExclusive()
				.process(
						new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(
									Bean1 left,
									Bean2 right,
									ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>.Context context,
									Collector<Tuple2<Bean1, Bean2>> collector)
									throws Exception {
								collector.collect(new Tuple2<Bean1, Bean2>(
										left, right));
							}

						});
		joinDs.print();
		env.execute();

	}

}

package com.sdps.flink.realtime.util;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class MyKafkaUtil {
	private static String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
	private static String DEFAULT_TOPIC = "dwd_default_topic";
	private static Properties properties = new Properties();
	static {
		properties.setProperty("bootstrap.servers", brokers);
	}

	public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
		return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(),
				properties);
	}

	public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,
			String groupId) {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),
				properties);
	}

	public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(
			KafkaSerializationSchema<T> kafkaSerializationSchema) {
		properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
				5 * 60 * 1000 + "");
		return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
				kafkaSerializationSchema, properties,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
	}

	// 拼接 Kafka 相关属性到 DDL
	public static String getKafkaDDL(String topic, String groupId) {
		String ddl = "'connector' = 'kafka', " + " 'topic' = '" + topic + "',"
				+ " 'properties.bootstrap.servers' = '" + brokers + "', "
				+ " 'properties.group.id' = '" + groupId + "', "
				+ " 'format' = 'json', "
				+ " 'scan.startup.mode' = 'latest-offset' ";
		return ddl;
	}
}
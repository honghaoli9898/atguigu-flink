package com.sdps.flink.cdc;

import java.util.Objects;

import io.debezium.data.Envelope;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;

public class CustomerDeserilzation implements
		DebeziumDeserializationSchema<String> {

	private static final long serialVersionUID = 6036305744551932363L;

	@Override
	public TypeInformation<String> getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}

	@Override
	public void deserialize(SourceRecord sourceRecord,
			Collector<String> collector) throws Exception {
		// 获取主题信息,包含着数据库和表名
		String topic = sourceRecord.topic();
		String[] arr = topic.split("\\.");
		String db = arr[1];
		String tableName = arr[2];
		// 获取操作类型 READ DELETE UPDATE CREATE
		Envelope.Operation operation = Envelope.operationFor(sourceRecord);
		String type = operation.toString().toLowerCase();
		if(Objects.equals(type, "create")){
			type= "insert";
		}
		// 获取值信息并转换为 Struct 类型
		Struct value = (Struct) sourceRecord.value();
		// 获取变化后的数据
		Struct after = value.getStruct("after");
		// 创建 JSON 对象用于存储数据信息
		JSONObject afterData = new JSONObject();
		if (Objects.nonNull(after)) {
			for (Field field : after.schema().fields()) {
				Object o = after.get(field);
				afterData.put(field.name(), o);
			}
		}
		Struct before = value.getStruct("before");
		// 创建 JSON 对象用于存储数据信息
		JSONObject beforeData = new JSONObject();
		if (Objects.nonNull(before)) {
			for (Field field : after.schema().fields()) {
				Object o = after.get(field);
				beforeData.put(field.name(), o);
			}
		}
		// 创建 JSON 对象用于封装最终返回值数据信息
		JSONObject result = new JSONObject();
		result.put("operation", type);
		result.put("after", afterData);
		result.put("before", beforeData);
		result.put("database", db);
		result.put("table", tableName);
		// 发送数据至下游
		collector.collect(result.toJSONString());
	}

}

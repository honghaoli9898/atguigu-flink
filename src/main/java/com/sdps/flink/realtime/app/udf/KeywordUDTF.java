package com.sdps.flink.realtime.app.udf;

import java.util.List;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.sdps.flink.realtime.util.KeywordUtil;

/**
 * Desc: 自定义 UDTF 函数实现分词功能
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
	private static final long serialVersionUID = 1L;

	public void eval(String value) {
		try {

			List<String> keywordList = KeywordUtil.analyze(value, true);
			for (String keyword : keywordList) {
				Row row = new Row(1);
				row.setField(0, keyword);
				collect(row);
			}
		} catch (Exception e) {
			collect(Row.of(value));
		}
	}
}
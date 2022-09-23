package com.sdps.flink.realtime.bean;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
/**
* Desc: 向 ClickHouse 写入数据的时候，如果有字段数据不需要传输，可以用该注解标记
*/
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
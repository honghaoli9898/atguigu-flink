package com.sdps.flink.realtime.bean;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderInfo {
	private Long id;
	private Long province_id;
	private String order_status;
	private Long user_id;
	private BigDecimal total_amount;
	private BigDecimal activity_reduce_amount;
	private BigDecimal coupon_reduce_amount;
	private BigDecimal original_total_amount;
	private BigDecimal feight_fee;
	private String expire_time;
	private String create_time;
	private String operate_time;
	private String create_date; // 把其他字段处理得到
	private String create_hour;
	private Long create_ts;
}

package com.sdps.flink.realtime.bean;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderDetail {
	private Long id;
	private Long order_id;
	private Long sku_id;
	private BigDecimal order_price;
	private Long sku_num;
	private String sku_name;
	private String create_time;
	private BigDecimal split_total_amount;
	private BigDecimal split_activity_amount;
	private BigDecimal split_coupon_amount;
	private Long create_ts;
}
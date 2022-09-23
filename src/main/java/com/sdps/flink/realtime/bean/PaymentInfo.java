package com.sdps.flink.realtime.bean;

import java.math.BigDecimal;

import lombok.Data;

@Data
public class PaymentInfo {
	Long id;
	Long order_id;
	Long user_id;
	BigDecimal total_amount;
	String subject;
	String payment_type;
	String create_time;
	String callback_time;
}
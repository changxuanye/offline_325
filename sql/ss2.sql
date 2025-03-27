drop table if exists `ods_payment_info`;
create table  `ods_payment_info`(
                                    id BIGINT COMMENT '编号',
                                    out_trade_no STRING COMMENT '对外业务编号',
                                    order_id BIGINT COMMENT '订单编号',
                                    user_id BIGINT COMMENT '用户编号',
                                    alipay_trade_no STRING COMMENT '支付宝交易流水编号',
                                    total_amount DECIMAL(16, 2) COMMENT '支付金额',
                                    subject STRING COMMENT '交易内容',
                                    payment_type STRING COMMENT '支付方式',
                                    payment_time TIMESTAMP COMMENT '支付时间'
)  COMMENT '支付流水表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_payment_info/';

load data  inpath '/2207A/xuanye_chang/payment_info/2025-03-23' overwrite into table ods_payment_info partition (dt='2025-03-23');
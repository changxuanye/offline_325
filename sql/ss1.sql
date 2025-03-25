create database dev_realtime_v1_xuanye_chang;
use dev_realtime_v1_xuanye_chang;
drop table ods_order_info;
create external table ods_order_info (
    id BIGINT COMMENT '编号',
    total_amount DECIMAL(16, 2) COMMENT '总金额',
    order_status STRING COMMENT '订单状态',
    user_id BIGINT COMMENT '用户id',
    payment_way STRING COMMENT '订单备注',
    out_trade_no STRING COMMENT '订单交易编号（第三方支付用)',
    create_time TIMESTAMP COMMENT '创建时间',
    operate_time TIMESTAMP COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY (`dt` string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
location '/warehouse/xuanye/ods/ods_order_info/' -- 指定数据在hdfs上的存储位置
;

load data  inpath '/2207A/xuanye_chang/order_info/2025-03-23' overwrite into table ods_order_info partition (dt='2025-03-23');



drop table if exists ods_order_detail;
create table ods_order_detail(
    id BIGINT COMMENT '编号',
    order_id BIGINT COMMENT '订单编号',
    user_id BIGINT COMMENT '用户id',
    sku_id BIGINT COMMENT 'sku_id',
    sku_name STRING COMMENT 'sku名称（冗余)',
    img_url STRING COMMENT '图片名称（冗余)',
    order_price DECIMAL(10, 2) COMMENT '购买价格(下单时sku价格）',
    sku_num STRING COMMENT '购买个数',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_order_detail/'
tblproperties ("parquet.compression"="snappy")
;

load data  inpath '/2207A/xuanye_chang/order_detail/2025-03-23' overwrite into table ods_order_detail partition (dt='2025-03-23');

drop table if exists ods_sku_info;
create table ods_sku_info(
id BIGINT COMMENT 'skuid(itemID)',
    spu_id BIGINT COMMENT 'spuid',
    price DECIMAL COMMENT '价格',
    sku_name STRING COMMENT 'sku名称',
    sku_desc STRING COMMENT '商品规格描述',
    weight DECIMAL(10, 2) COMMENT '重量',
    tm_id BIGINT COMMENT '品牌(冗余)',
    category3_id BIGINT COMMENT '三级分类id（冗余)',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_sku_info/'
;


load data  inpath '/2207A/xuanye_chang/sku_info/2025-03-23' overwrite into table ods_sku_info partition (dt='2025-03-23');


drop table if exists ods_user_info;
create table ods_user_info(
    id BIGINT COMMENT '编号',
    name STRING COMMENT '用户姓名',
    birthday DATE COMMENT '用户生日',
    gender STRING COMMENT '性别 M男,F女',
    email STRING COMMENT '邮箱',
    user_level STRING COMMENT '用户级别',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '用户信息'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_user_info/'
;

load data  inpath '/2207A/xuanye_chang/user_info/2025-03-23' overwrite into table ods_user_info partition (dt='2025-03-23');



drop table if exists ods_base_category1;
create table ods_base_category1(
   `id` string COMMENT 'id',
   `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_base_category1/'
;

load data  inpath '/2207A/xuanye_chang/base_category1/2025-03-23' overwrite into table ods_base_category1 partition (dt='2025-03-23');



drop table if exists ods_base_category2;
CREATE EXTERNAL TABLE ods_base_category2 (
    id BIGINT COMMENT '编号',
    name STRING COMMENT '二级分类名称',
    category1_id BIGINT COMMENT '一级分类编号'
)
COMMENT '商品二级分类'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/xuanye/ods/ods_base_category2/';


load data  inpath '/2207A/xuanye_chang/base_category2/2025-03-23' overwrite into table ods_base_category2 partition (dt='2025-03-23');




drop table if exists ods_base_category3;
create table ods_base_category3(
   `id` string COMMENT ' id',
   `name`  string COMMENT '名称',
  category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_base_category3/'
;

load data  inpath '/2207A/xuanye_chang/base_category3/2025-03-23' overwrite into table ods_base_category3 partition (dt='2025-03-23');


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

drop table if exists ods_comment_info;
create external table ods_comment_info(
    id BIGINT COMMENT '编号',
    user_id BIGINT COMMENT '用户名称',
    sku_id BIGINT COMMENT 'skuid',
    spu_id BIGINT COMMENT '商品id',
    order_id BIGINT COMMENT '订单编号',
    appraise STRING COMMENT '评价 1 好评 2 中评 3 差评',
    comment_txt STRING COMMENT '评价内容',
    create_time TIMESTAMP COMMENT '创建时间',
    operate_time TIMESTAMP COMMENT '修改时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/xuanye/ods/ods_comment_info/';

load data  inpath '/2207A/xuanye_chang/comment_info/2025-03-23' overwrite into table ods_comment_info partition (dt='2025-03-23');

-- dwd
drop table if exists dwd_order_info;
create external table dwd_order_info (
id BIGINT COMMENT '编号',
    total_amount DECIMAL(16, 2) COMMENT '总金额',
    order_status STRING COMMENT '订单状态',
    user_id BIGINT COMMENT '用户id',
    payment_way STRING COMMENT '订单备注',
    out_trade_no STRING COMMENT '订单交易编号（第三方支付用)',
    create_time TIMESTAMP COMMENT '创建时间',
    operate_time TIMESTAMP COMMENT '操作时间'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy")
;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_order_info partition (dt)
select * from ods_order_info
where dt = '2025-03-23' and id is not null;




drop table if exists dwd_order_detail;
create external table dwd_order_detail(
  id BIGINT COMMENT '编号',
    order_id BIGINT COMMENT '订单编号',
    user_id BIGINT COMMENT '用户id',
    sku_id BIGINT COMMENT 'sku_id',
    sku_name STRING COMMENT 'sku名称（冗余)',
    img_url STRING COMMENT '图片名称（冗余)',
    order_price DECIMAL(10, 2) COMMENT '购买价格(下单时sku价格）',
    sku_num STRING COMMENT '购买个数',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy")
;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_order_detail partition (dt)
select * from ods_order_detail
where dt = '2025-03-23' and id is not null;

select * from dwd_order_detail;

drop table if exists dwd_user_info;
create external table dwd_user_info(
    id BIGINT COMMENT '编号',
    name STRING COMMENT '用户姓名',
    birthday DATE COMMENT '用户生日',
    gender STRING COMMENT '性别 M男,F女',
    email STRING COMMENT '邮箱',
    user_level STRING COMMENT '用户级别',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_user_info partition (dt)
select * from ods_user_info
where dt = '2025-03-23' and id is not null;


drop table if exists `dwd_payment_info`;
create external  table  `dwd_payment_info`(
    id BIGINT COMMENT '编号',
    out_trade_no STRING COMMENT '对外业务编号',
    order_id BIGINT COMMENT '订单编号',
    user_id BIGINT COMMENT '用户编号',
    alipay_trade_no STRING COMMENT '支付宝交易流水编号',
    total_amount DECIMAL(16, 2) COMMENT '支付金额',
    subject STRING COMMENT '交易内容',
    payment_type STRING COMMENT '支付方式',
    payment_time TIMESTAMP COMMENT '支付时间'
 )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_payment_info partition (dt)
select * from ods_payment_info
where dt = '2025-03-23' and id is not null;

drop table if exists dwd_sku_info;
create external table dwd_sku_info(
id BIGINT COMMENT 'skuid(itemID)',
    spu_id BIGINT COMMENT 'spuid',
    price DECIMAL COMMENT '价格',
    sku_name STRING COMMENT 'sku名称',
    sku_desc STRING COMMENT '商品规格描述',
    weight DECIMAL(10, 2) COMMENT '重量',
    tm_id BIGINT COMMENT '品牌(冗余)',
    category3_id BIGINT COMMENT '三级分类id（冗余)',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_sku_info partition (dt)
select * from ods_sku_info
where dt = '2025-03-23' and id is not null;



create external table dwd_base_category1(
 `id` string COMMENT 'id',
   `name`  string COMMENT '名称'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_base_category1/'
tblproperties ("parquet.compression"="snappy")
;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_base_category1 partition (dt)
select * from ods_base_category1
where dt = '2025-03-23' and id is not null;



create external table dwd_base_category2(
id BIGINT COMMENT '编号',
    name STRING COMMENT '二级分类名称',
    category1_id BIGINT COMMENT '一级分类编号'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_base_category2/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_base_category2 partition (dt)
select * from ods_base_category2
where dt = '2025-03-23' and id is not null;




create external table dwd_base_category3(
   `id` string COMMENT ' id',
   `name`  string COMMENT '名称',
  category2_id string COMMENT '二级品类id'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_base_category3/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_base_category3 partition (dt)
select * from ods_base_category3
where dt = '2025-03-23' and id is not null;



create external table dwd_comment_info(
    id BIGINT COMMENT '编号',
    user_id BIGINT COMMENT '用户名称',
    sku_id BIGINT COMMENT 'skuid',
    spu_id BIGINT COMMENT '商品id',
    order_id BIGINT COMMENT '订单编号',
    appraise STRING COMMENT '评价 1 好评 2 中评 3 差评',
    comment_txt STRING COMMENT '评价内容',
    create_time TIMESTAMP COMMENT '创建时间',
    operate_time TIMESTAMP COMMENT '修改时间'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dwd/dwd_comment_info/'
tblproperties ("parquet.compression"="snappy")
;

insert into table dwd_comment_info partition (dt)
select * from ods_comment_info
where dt = '2025-03-23' and id is not null;

--dws

drop table if exists dws_user_action;
create  external table dws_user_action
(
    user_id         string      comment '用户 id',
    order_count     bigint      comment '下单次数 ',
    order_amount    decimal(16,2)  comment '下单金额 ',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额 '
) COMMENT '每日用户行为宽表'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dws/dws_user_action/'
tblproperties ("parquet.compression"="snappy");


with
tmp_order as
(
    select
        user_id,
        sum(oc.total_amount) order_amount,
        count(*)  order_count
    from dwd_order_info  oc
    where date_format(oc.create_time,'yyyy-MM-dd')='2020-04-01'
    group by user_id
)  ,
tmp_payment as
(
    select
        user_id,
        sum(pi.total_amount) payment_amount,
        count(*) payment_count
    from dwd_payment_info pi
    where date_format(pi.payment_time,'yyyy-MM-dd')='2020-04-01'
    group by user_id
)
insert overwrite table dws_user_action partition(dt='2025-03-23')
select
    user_actions.user_id,
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount)
from
(
    select
        user_id,
        order_count,
        order_amount ,
        0 payment_count ,
        0 payment_amount,
        0 comment_count
    from tmp_order
    union all
    select
        user_id,
        0,
        0,
        payment_count,
        payment_amount,
        0
    from tmp_payment
 ) user_actions
group by user_id;

drop table if exists dws_sale_detail_daycount;
create external table  dws_sale_detail_daycount
(  user_id  string  comment '用户 id',
 sku_id  string comment '商品 Id',
 user_gender  string comment '用户性别',
 user_age string  comment '用户年龄',
 user_level string comment '用户等级',
 order_price decimal(10,2) comment '订单价格',
 sku_name string  comment '商品名称',
 sku_tm_id string comment '品牌id',
 spu_id  string comment '商品 spu',
 sku_num  int comment '购买个数',
 order_count string comment '当日下单单数',
 order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/xuanye/dws/dws_user_sale_detail_daycount/'
tblproperties ("parquet.compression"="snappy");



with
tmp_detail as
(
    select
        user_id,
        sku_id,
        sum(sku_num) sku_num1 ,
        count(*) order_count ,
        sum(od.order_price*sku_num)  order_amount
    from ods_order_detail od
    where od.dt='2025-03-23' and user_id is not null
    group by user_id, sku_id
)
insert overwrite table  dws_sale_detail_daycount partition(dt='2025-03-23')
select
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('2025-03-23', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    spu_id,
    tmp_detail.sku_num1,
    tmp_detail.order_count,
    tmp_detail.order_amount
from tmp_detail
left join dwd_user_info u on u.id=tmp_detail.user_id  and u.dt='2025-03-23'
left join dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='2025-03-23';

create  table ads_sale_tm_category1_stat_mn
(
    tm_id string comment '品牌id ' ,
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率' ,
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期'
)   COMMENT '复购率统计'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/'
;


insert into table ads_sale_tm_category1_stat_mn
select
    mn.sku_tm_id,
    sum(if(mn.order_count>=1,1,0)) buycount,
    sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
    sum(if(mn.order_count>=2,1,0))/sum( if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(mn.order_count>3,1,0))  buy3timeLast  ,
    sum(if(mn.order_count>=3,1,0))/sum( if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2019-02-10' ,'yyyy-MM') stat_mn,
    '2019-02-10' stat_date
from
(
    select od.sku_tm_id,
        user_id ,
        sum(order_count) order_count
    from  dws_sale_detail_daycount  od
    where
        date_format(dt,'yyyy-MM')<=date_format('2019-02-10' ,'yyyy-MM')
    group by
        od.sku_tm_id, user_id
) mn
group by mn.sku_tm_id
;














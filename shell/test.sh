#coding=utf-8
import os
import sys
import getopt
import json
import MySQLdb

#MySQL相关配置，需根据实际情况作出修改
mysql_host = "10.39.48.36"
mysql_port = "3306"
mysql_user = "root"
mysql_passwd = "zh1028,./"

#HDFS NameNode相关配置，需根据实际情况作出修改
hdfs_nn_host = "10.39.48.30"
hdfs_nn_port = "9870"

#生成配置文件的目标路径，可根据实际情况作出修改
output_path = "/opt/soft/datax/job/lihao_song/import"

def get_connection():
    return MySQLdb.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, passwd=mysql_passwd)

def get_mysql_meta(database, table):
    connection = get_connection()
    cursor = connection.cursor()
    sql= "SELECT COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION"
    cursor.execute(sql, [database, table])
    fetchall = cursor.fetchall()
    cursor.close()
    connection.close()
    return fetchall


def get_mysql_columns(database, table):
    return (list(map(lambda x: x[0], get_mysql_meta(database, table))))


def get_hive_columns(database, table):
    def type_mapping(mysql_type):
        mappings = {
            "bigint": "bigint",
            "int": "bigint",
            "smallint": "bigint",
            "tinyint": "bigint",
            "decimal": "string",
            "double": "double",
            "float": "float",
            "binary": "string",
            "char": "string",
            "varchar": "string",
            "datetime": "string",
            "time": "string",
            "timestamp": "string",
            "date": "string",
            "text": "string"
        }
        return mappings[mysql_type]

    meta = get_mysql_meta(database, table)
    return  (list(map(lambda x: {"name": x[0], "type": type_mapping(x[1].lower())}, meta)))


def generate_json(source_database, source_table):
    job = {
        "job": {
            "setting": {
                "speed": {
                    "channel": 3
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            },
            "content": [{
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": mysql_user,
                        "password": mysql_passwd,
                        "column": get_mysql_columns(source_database, source_table),
                        "splitPk": "",
                        "connection": [{
                            "table": [source_table],
                            "jdbcUrl": ["jdbc:mysql://" + mysql_host + ":" + mysql_port + "/" + source_database + "?useSSL=false"]
                        }]
                    }
                },
                "writer": {
                    "name": "hdfswriter",
                    "parameter": {
                        "defaultFS": "hdfs://" + hdfs_nn_host + ":" + hdfs_nn_port,
                        "fileType": "text",
                        "path": "${targetdir}",
                        "fileName": source_table,
                        "column": get_hive_columns(source_database, source_table),
                        "writeMode": "append",
                        "fieldDelimiter": "\t",
                        "compress": "gzip"
                    }
                }
            }]
        }
    }
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    with open(os.path.join(output_path, ".".join([source_database, source_table, "json"])), "w") as f:
        json.dump(job, f)


def main(args):
    source_database = "db_gmall"
    source_table = "activity_rule"

    options, arguments = getopt.getopt(args, '-d:-t:', ['sourcedb=', 'sourcetbl='])
    for opt_name, opt_value in options:
        if opt_name in ('-d', '--sourcedb'):
            source_database = opt_value
        if opt_name in ('-t', '--sourcetbl'):
            source_table = opt_value

    generate_json(source_database, source_table)


if __name__ == '__main__':
    main(sys.argv[1:])


#!/bin/bash
python gen_import_config.py -d dev_realtime_v1_lihao_song -t order_detail
python gen_import_config.py -d dev_realtime_v1_lihao_song -t order_info
python gen_import_config.py -d dev_realtime_v1_lihao_song -t sku_info
python gen_import_config.py -d dev_realtime_v1_lihao_song -t user_info


#3-25
-- ods建表
create table ods_order_info (
   `id` string COMMENT '订单编号',
   `total_amount` decimal(10,2) COMMENT '订单金额',
   `order_status` string COMMENT '订单状态',
  `user_id` string COMMENT '用户id' ,
 `payment_way` string COMMENT '支付方式',
 `out_trade_no` string COMMENT '支付流水号',
 `create_time` string COMMENT '创建时间',
 `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/ods/ods_order_info/'
tblproperties ("parquet.compression"="snappy");

create table ods_order_detail(
   `id` string COMMENT '订单编号',
   `order_id` string  COMMENT '订单号',
  `user_id` string COMMENT '用户id' ,
 `sku_id` string COMMENT '商品id',
 `sku_name` string COMMENT '商品名称',
 `order_price` string COMMENT '下单价格',
 `sku_num` string COMMENT '商品数量',
 `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/ods/ods_order_detail/'
tblproperties ("parquet.compression"="snappy");

create table ods_sku_info(
   `id` string COMMENT 'skuId',
   `spu_id` string  COMMENT 'spuid',
  `price` decimal(10,2) COMMENT '价格' ,
 `sku_name` string COMMENT '商品名称',
 `sku_desc` string COMMENT '商品描述',
 `weight` string COMMENT '重量',
 `tm_id` string COMMENT '品牌id',
 `category3_id` string COMMENT '品类id',
 `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/ods/ods_sku_info/'
tblproperties ("parquet.compression"="snappy");

create table ods_user_info(
   `id` string COMMENT '用户id',
   `name`  string COMMENT '姓名',
  `birthday` string COMMENT '生日' ,
 `gender` string COMMENT '性别',
 `email` string COMMENT '邮箱',
 `user_level` string COMMENT '用户等级',
 `create_time` string COMMENT '创建时间'
) COMMENT '用户信息'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/ods/ods_user_info/'
tblproperties ("parquet.compression"="snappy");
--  ods数据导入
load data inpath '/origin_data/db/order_info/2025-03-24'
OVERWRITE into table ods_order_info partition(dt='2025-03-24');
load data inpath '/origin_data/db/order_detail/2025-03-24'
OVERWRITE into table ods_order_detail partition(dt='2025-03-24');
load data inpath '/origin_data/db/sku_info/2025-03-24'
OVERWRITE into table ods_sku_info partition(dt='2025-03-24');
load data inpath '/origin_data/db/user_info/2025-03-24'
OVERWRITE into table ods_user_info partition(dt='2025-03-24');
-- dwd建表
create external table dwd_order_info (
  `id` string COMMENT '',
   `total_amount` decimal(10,2) COMMENT '',
   `order_status` string COMMENT ' 1 2  3  4  5',
  `user_id` string COMMENT 'id' ,
 `payment_way` string COMMENT '',
 `out_trade_no` string COMMENT '',
 `create_time` string COMMENT '',
 `operate_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_lihao_song/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy");

create external table dwd_order_detail(
   `id` string COMMENT '',
   `order_id` decimal(10,2) COMMENT '',
  `user_id` string COMMENT 'id' ,
 `sku_id` string COMMENT 'id',
 `sku_name` string COMMENT '',
 `order_price` string COMMENT '',
 `sku_num` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_lihao_song/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy");

create external table dwd_user_info(
   `id` string COMMENT 'id',
   `name`  string COMMENT '',
  `birthday` string COMMENT '' ,
 `gender` string COMMENT '',
 `email` string COMMENT '',
 `user_level` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_lihao_song/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy");

create table dwd_sku_info(
   `id` string COMMENT 'skuId',
   `spu_id` string  COMMENT 'spuid',
  `price` decimal(10,2) COMMENT '价格' ,
 `sku_name` string COMMENT '商品名称',
 `sku_desc` string COMMENT '商品描述',
 `weight` string COMMENT '重量',
 `tm_id` string COMMENT '品牌id',
 `category3_id` string COMMENT '品类id',
 `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy");
-- dwd数据
set hive.exec.dynamic.partition.mode=nonstrict;
insert  overwrite table   dwd_order_info partition(dt )
select  * from ods_order_info
where dt='2025-03-24'  and id is not null;
insert  overwrite table   dwd_order_detail partition(dt )
select  * from ods_order_detail
where dt='2025-03-24'  and id is not null;
insert  overwrite table   dwd_sku_info partition(dt )
select  * from ods_sku_info
where dt='2025-03-24'  and id is not null;
insert  overwrite table   dwd_user_info partition(dt )
select  * from ods_user_info
where dt='2025-03-24'  and id is not null;


drop table dws_sale_detail;
-- dws
create table  dws_sale_detail
(  user_id  string  comment '用户 id',
 sku_id  string comment '商品 Id',
 user_gender  string comment '用户性别',
 user_age string  comment '用户年龄',
 user_level string comment '用户等级',
 order_price decimal(10,2) comment '订单价格',
 sku_name string  comment '商品名称',
 sku_tm_id string  comment '品牌id',
 spu_id  string comment '商品 spu',
 sku_num  int comment '购买个数',
 order_count string comment '当日下单单数',
 order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_lihao_song/dws/dws_sale_detail/'
tblproperties ("parquet.compression"="snappy");


#0326
with
tmp_detail as
(
    select
        user_id,
        sku_id,
        sum(sku_num) sku_num ,
        count(*) order_count ,
        sum(od.order_price*sku_num)  order_amount
    from ods_order_detail od
    where od.dt='2025-03-25' and user_id is not null
    group by user_id, sku_id
)
insert overwrite table  dws_sale_detail partition(dt='2025-03-25')
select
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('2025-03-25', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    spu_id,
    tmp_detail.sku_num,
    tmp_detail.order_count,
    tmp_detail.order_amount
from tmp_detail
left join dwd_user_info u on u.id=tmp_detail.user_id  and u.dt='2025-03-25'
left join dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='2025-03-25';

-- ads
drop  table ads_goods_purchase_rate;
create  table ads_goods_purchase_rate(
sku_id string,
sku_name string,
buycount bigint,
buy_twice_last bigint,
buy_twice_last_ratio decimal(10,2),
buy_3times_last bigint,
buy_3times_last_ratio decimal(10,2),
stat_mn string,
stat_date string
)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_lihao_song/ads/ads_goods_purchase_rate';
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads_goods_purchase_rate
select
sku_id,
sku_name,
sum(if(order_count>=1,1,0)) buycount,
sum(if(order_count>=2,1,0)) buy_twice_count,
sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) buy_twice_rate,
sum(if(order_count>3,1,0)) buy_repeatedly_count,
sum(if(order_count>3,1,0))/sum(if(order_count>=1,1,0)) buy_repeatedly_rate,
date_format('2025-03-15','yyyy-MM') stat_mn,
date_format('2025-03-15','yyyy-MM-dd') stat_date
from
(select
user_id,
sku_id,
sku_name,
count(1) order_count,
sum(order_price*sku_num)
from dws_sale_detail
group by user_id,sku_id,sku_name) a
group by sku_id,sku_name;



######04
-- 广告销售日报汇总
CREATE TABLE ods_sales_summary_daily (
    id BIGINT COMMENT 'Primary key ID',
    category_id BIGINT COMMENT 'Category ID',
    category_name VARCHAR(255) COMMENT 'Category name',
    shop_id BIGINT COMMENT 'Shop ID',
    stat_date STRING COMMENT 'Statistics date',
    gmv DECIMAL(18,2) COMMENT 'Total sales amount',
    order_count BIGINT COMMENT 'Total order count',
    item_count BIGINT COMMENT 'Total sales volume',
    uv BIGINT COMMENT 'Visitor count',
    pay_uv BIGINT COMMENT 'Paying user count',
    pay_rate DECIMAL(10,4) COMMENT 'Payment conversion rate',
    create_time STRING COMMENT 'Creation time',
    update_time STRING COMMENT 'Update time'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_sales_summary_daily/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_sales_summary_daily PARTITION(dt='2025-03-31')
VALUES
-- 店铺1001的数据
(1, 101, '智能手机', 1001, '2025-03-31', 250000.00, 500, 600, 10000, 800, 0.0800, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 102, '智能手表', 1001, '2025-03-31', 120000.00, 300, 350, 6000, 450, 0.0750, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 103, '蓝牙耳机', 1001, '2025-03-31', 80000.00, 400, 500, 8000, 500, 0.0625, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 店铺1002的数据
(4, 101, '智能手机', 1002, '2025-03-31', 180000.00, 350, 420, 8500, 600, 0.0706, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(5, 104, '笔记本电脑', 1002, '2025-03-31', 220000.00, 180, 190, 5500, 220, 0.0400, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 105, '平板电脑', 1002, '2025-03-31', 110000.00, 250, 280, 5000, 300, 0.0600, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 店铺1003的数据
(7, 102, '智能手表', 1003, '2025-03-31', 95000.00, 250, 300, 5000, 350, 0.0700, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 103, '蓝牙耳机', 1003, '2025-03-31', 65000.00, 350, 450, 7000, 450, 0.0643, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(9, 106, '智能家居', 1003, '2025-03-31', 75000.00, 200, 250, 4500, 250, 0.0556, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 店铺1004的数据
(10, 104, '笔记本电脑', 1004, '2025-03-31', 300000.00, 200, 210, 6000, 250, 0.0417, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 107, '游戏主机', 1004, '2025-03-31', 180000.00, 150, 160, 4000, 180, 0.0450, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 108, '数码相机', 1004, '2025-03-31', 95000.00, 120, 130, 3500, 150, 0.0429, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告热销商品
CREATE TABLE ods_top_selling_items (
    id BIGINT COMMENT '主键ID',
    category_id BIGINT COMMENT '品类ID',
    item_id BIGINT COMMENT '商品ID',
    item_name VARCHAR(255) COMMENT '商品名称',
    shop_id BIGINT COMMENT '店铺ID',
    stat_date DATE COMMENT '统计日期',
    gmv DECIMAL(18,2) COMMENT '销售额',
    sale_count BIGINT COMMENT '销量',
    rank INT COMMENT '排名',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_top_selling_items/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_top_selling_items PARTITION(dt='2025-03-31')
VALUES
-- 智能手机品类热销商品
(1, 101, 2001, '旗舰智能手机X Pro', 1001, '2025-03-31', 150000.00, 300, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 101, 2002, '入门智能手机Y Lite', 1001, '2025-03-31', 100000.00, 200, 2, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 101, 2003, '旗舰智能手机Z Ultra', 1002, '2025-03-31', 120000.00, 200, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(4, 101, 2004, '中端智能手机A', 1002, '2025-03-31', 60000.00, 150, 2, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手表品类热销商品
(5, 102, 2005, '智能手表Pro Max', 1001, '2025-03-31', 80000.00, 200, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 102, 2006, '智能手表Lite', 1001, '2025-03-31', 40000.00, 100, 2, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(7, 102, 2007, '健康监测手表', 1003, '2025-03-31', 65000.00, 150, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 102, 2008, '运动智能手表', 1003, '2025-03-31', 30000.00, 100, 2, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 笔记本电脑品类热销商品
(9, 104, 2009, '商务笔记本 Elite', 1002, '2025-03-31', 120000.00, 100, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10, 104, 2010, '游戏笔记本 Pro', 1004, '2025-03-31', 180000.00, 80, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 104, 2011, '轻薄笔记本 Air', 1004, '2025-03-31', 120000.00, 120, 2, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 104, 2012, '学生笔记本 Basic', 1002, '2025-03-31', 60000.00, 80, 3, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告分类属性
CREATE TABLE ods_category_attributes (
    id BIGINT COMMENT '主键ID',
    category_id BIGINT COMMENT '品类ID',
    attribute_id BIGINT COMMENT '属性ID',
    attribute_name VARCHAR(255) COMMENT '属性名称',
    attribute_value VARCHAR(255) COMMENT '属性值',
    shop_id BIGINT COMMENT '店铺ID',
    stat_date DATE COMMENT '统计日期',
    uv BIGINT COMMENT '访客数',
    pay_uv BIGINT COMMENT '支付用户数',
    pay_rate DECIMAL(10,4) COMMENT '支付转化率',
    item_count BIGINT COMMENT '动销商品数',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_category_attributes/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_category_attributes PARTITION(dt='2025-03-31')
VALUES
-- 智能手机属性
(1, 101, 1, '内存容量', '8GB', 1001, '2025-03-31', 5000, 400, 0.0800, 15, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 101, 1, '内存容量', '12GB', 1001, '2025-03-31', 3000, 300, 0.1000, 10, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 101, 2, '存储容量', '128GB', 1001, '2025-03-31', 4000, 320, 0.0800, 12, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(4, 101, 2, '存储容量', '256GB', 1001, '2025-03-31', 3500, 280, 0.0800, 10, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手表属性
(5, 102, 3, '屏幕类型', 'AMOLED', 1001, '2025-03-31', 3000, 240, 0.0800, 8, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 102, 3, '屏幕类型', 'LCD', 1001, '2025-03-31', 2000, 160, 0.0800, 6, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(7, 102, 4, '防水等级', 'IP68', 1003, '2025-03-31', 2500, 200, 0.0800, 7, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 102, 4, '防水等级', 'IP67', 1003, '2025-03-31', 1500, 120, 0.0800, 5, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 笔记本电脑属性
(9, 104, 5, '处理器', 'i7', 1004, '2025-03-31', 2000, 160, 0.0800, 6, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10, 104, 5, '处理器', 'i5', 1004, '2025-03-31', 2500, 180, 0.0720, 7, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 104, 6, '显卡', '独立显卡', 1002, '2025-03-31', 1800, 140, 0.0778, 5, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 104, 6, '显卡', '集成显卡', 1002, '2025-03-31', 2200, 160, 0.0727, 6, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告流量来源
CREATE TABLE ods_traffic_source (
    id BIGINT COMMENT '主键ID',
    category_id BIGINT COMMENT '品类ID',
    shop_id BIGINT COMMENT '店铺ID',
    stat_date DATE COMMENT '统计日期',
    source_type VARCHAR(50) COMMENT '来源类型(搜索/推荐/广告等)',
    source_detail VARCHAR(255) COMMENT '来源详情',
    uv BIGINT COMMENT '访客数',
    pv BIGINT COMMENT '浏览量',
    pay_uv BIGINT COMMENT '支付用户数',
    pay_rate DECIMAL(10,4) COMMENT '支付转化率',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_traffic_source/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_traffic_source PARTITION(dt='2025-03-31')
VALUES
-- 店铺1001的流量来源
(1, 101, 1001, '2025-03-31', '搜索', '品牌关键词搜索', 4000, 8000, 320, 0.0800, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 101, 1001, '2025-03-31', '推荐', '首页推荐', 3000, 6000, 240, 0.0800, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 101, 1001, '2025-03-31', '广告', '信息流广告', 2000, 4000, 160, 0.0800, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(4, 101, 1001, '2025-03-31', '社交', '社交媒体引流', 1000, 2000, 80, 0.0800, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 店铺1002的流量来源
(5, 104, 1002, '2025-03-31', '搜索', '品类关键词搜索', 2500, 5000, 100, 0.0400, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 104, 1002, '2025-03-31', '推荐', '猜你喜欢', 2000, 4000, 80, 0.0400, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(7, 104, 1002, '2025-03-31', '广告', '搜索广告', 1500, 3000, 60, 0.0400, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 104, 1002, '2025-03-31', '直接访问', '直接输入网址', 500, 1000, 20, 0.0400, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 店铺1003的流量来源
(9, 102, 1003, '2025-03-31', '搜索', '品牌关键词搜索', 2000, 4000, 140, 0.0700, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10, 102, 1003, '2025-03-31', '推荐', '同类商品推荐', 1500, 3000, 105, 0.0700, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 102, 1003, '2025-03-31', '广告', '展示广告', 1000, 2000, 70, 0.0700, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 102, 1003, '2025-03-31', '社交', 'KOL推荐', 500, 1000, 35, 0.0700, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告热门搜索关键词
CREATE TABLE ods_hot_search_keywords (
    id BIGINT COMMENT '主键ID',
    category_id BIGINT COMMENT '品类ID',
    shop_id BIGINT COMMENT '店铺ID',
    stat_date string COMMENT '统计日期',
    keyword VARCHAR(255) COMMENT '搜索关键词',
    search_count BIGINT COMMENT '搜索次数',
    uv BIGINT COMMENT '访客数',
    click_rate DECIMAL(10,4) COMMENT '点击率',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_hot_search_keywords/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_hot_search_keywords PARTITION(dt='2025-03-31')
VALUES
-- 智能手机相关关键词
(1, 101, 1001, '2025-03-31', '旗舰手机', 1500, 1200, 0.8000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 101, 1001, '2025-03-31', '5G手机', 1200, 900, 0.7500, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 101, 1001, '2025-03-31', '大屏手机', 800, 600, 0.7500, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(4, 101, 1002, '2025-03-31', '拍照手机', 1000, 700, 0.7000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手表相关关键词
(5, 102, 1001, '2025-03-31', '运动手表', 900, 630, 0.7000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 102, 1001, '2025-03-31', '健康监测', 800, 560, 0.7000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(7, 102, 1003, '2025-03-31', '长续航手表', 700, 490, 0.7000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 102, 1003, '2025-03-31', '防水手表', 600, 420, 0.7000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 笔记本电脑相关关键词
(9, 104, 1004, '2025-03-31', '轻薄本', 800, 320, 0.4000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10, 104, 1004, '2025-03-31', '游戏本', 700, 280, 0.4000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 104, 1002, '2025-03-31', '商务笔记本', 600, 240, 0.4000, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 104, 1002, '2025-03-31', '高性价比笔记本', 500, 200, 0.4000, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告用户画像
CREATE TABLE ods_user_portrait (
    id BIGINT COMMENT '主键ID',
    category_id BIGINT COMMENT '品类ID',
    shop_id BIGINT COMMENT '店铺ID',
    stat_date string COMMENT '统计日期',
    user_behavior VARCHAR(20) COMMENT '用户行为(search/visit/pay)',
    gender VARCHAR(10) COMMENT '性别',
    age_range VARCHAR(20) COMMENT '年龄段',
    province VARCHAR(50) COMMENT '省份',
    city VARCHAR(50) COMMENT '城市',
    user_count BIGINT COMMENT '用户数',
    gmv DECIMAL(18,2) COMMENT '销售额',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_user_portrait/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_user_portrait PARTITION(dt='2025-03-31')
VALUES
-- 智能手机用户画像
(1, 101, 1001, '2025-03-31', 'pay', '男', '25-30岁', '广东', '深圳', 320, 128000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(2, 101, 1001, '2025-03-31', 'pay', '女', '25-30岁', '上海', '上海', 240, 96000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(3, 101, 1001, '2025-03-31', 'pay', '男', '31-35岁', '北京', '北京', 160, 64000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(4, 101, 1001, '2025-03-31', 'visit', '女', '20-24岁', '浙江', '杭州', 2000, 0.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手表用户画像
(5, 102, 1003, '2025-03-31', 'pay', '女', '25-30岁', '广东', '广州', 140, 49000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(6, 102, 1003, '2025-03-31', 'pay', '男', '31-35岁', '江苏', '南京', 105, 36750.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(7, 102, 1003, '2025-03-31', 'pay', '男', '25-30岁', '四川', '成都', 70, 24500.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(8, 102, 1003, '2025-03-31', 'search', '女', '20-24岁', '湖北', '武汉', 1500, 0.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 笔记本电脑用户画像
(9, 104, 1004, '2025-03-31', 'pay', '男', '18-24岁', '广东', '深圳', 100, 120000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10, 104, 1004, '2025-03-31', 'pay', '男', '25-30岁', '上海', '上海', 80, 96000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(11, 104, 1004, '2025-03-31', 'pay', '女', '18-24岁', '北京', '北京', 60, 72000.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(12, 104, 1004, '2025-03-31', 'visit', '男', '31-35岁', '浙江', '杭州', 1200, 0.00, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告分类信息
CREATE TABLE ods_category_info (
    category_id BIGINT COMMENT '品类ID',
    category_name VARCHAR(100) COMMENT '品类名称',
    parent_category_id BIGINT COMMENT '父品类ID',
    parent_category_name VARCHAR(100) COMMENT '父品类名称',
    level INT COMMENT '品类层级',
    is_leaf TINYINT COMMENT '是否叶子品类',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_category_info/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_category_info PARTITION(dt='2025-03-31')
VALUES
-- 一级分类
(101, '智能手机', 0, '全部', 1, 0, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(102, '智能手表', 0, '全部', 1, 0, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(103, '蓝牙耳机', 0, '全部', 1, 0, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(104, '笔记本电脑', 0, '全部', 1, 0, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手机子分类
(10101, '旗舰手机', 101, '智能手机', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10102, '中端手机', 101, '智能手机', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10103, '入门手机', 101, '智能手机', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 智能手电子分类
(10201, '运动手表', 102, '智能手表', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10202, '健康监测手表', 102, '智能手表', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10203, '儿童手表', 102, '智能手表', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),

-- 笔记本电脑子分类
(10401, '游戏本', 104, '笔记本电脑', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10402, '商务本', 104, '笔记本电脑', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(10403, '轻薄本', 104, '笔记本电脑', 2, 1, '2025-03-31 08:00:00', '2025-03-31 08:00:00');
-- 广告店铺信息
CREATE TABLE ods_shop_info (
    shop_id BIGINT COMMENT '店铺ID',
    shop_name VARCHAR(100) COMMENT '店铺名称',
    shop_level INT COMMENT '店铺等级',
    main_category VARCHAR(50) COMMENT '主营类目',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_360/ods/ods_shop_info/'
tblproperties (  "parquet.compression"="SNAPPY");
INSERT INTO TABLE ods_shop_info PARTITION(dt='2025-03-31')
VALUES
(1001, '旗舰数码商城', 5, '智能手机', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1002, '科技生活馆', 4, '笔记本电脑', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1003, '智能穿戴专家', 4, '智能手表', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1004, '高端电子世界', 5, '游戏设备', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1005, '平价数码超市', 3, '蓝牙耳机', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1006, '办公设备中心', 4, '办公设备', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1007, '摄影器材专卖', 4, '数码相机', '2025-03-31 08:00:00', '2025-03-31 08:00:00'),
(1008, '家电综合商城', 5, '家用电器', '2025-03-31 08:00:00', '2025-03-31 08:00:00');

-- 指标
-- 月支付金额进度
create table ads_month_payment(
    category_name string,
    total_gmv decimal(10,2),
    total_orders int,
    total_items int,
    total_uv int,
    total_pay_uv int
);
insert overwrite table ads_month_payment
select a.category_name,
       sum(gmv) as total_gmv,
       sum(order_count) as total_orders,
       sum(item_count) as total_items,
       sum(uv) as total_uv,
       sum(pay_uv) as total_pay_uv
from ods_sales_summary_daily a
join ods_category_info b
on a.category_id=b.parent_category_id
group by a.category_name
order by total_gmv desc;
-- 月支付金额贡献
create table ads_month_money(
    category_name string,
    item_name string,
    gmv decimal(10,2),
    sale_count int,
    rank int
);
insert overwrite table ads_month_money
select
    category_name,
    item_name,
    gmv,
    sale_count,
    rank
from ods_top_selling_items a
join ods_category_info b
on a.category_id=b.category_id
where a.dt='2025-03-31'
and rank<=10
order by category_name,rank;
-- 上月本店支付金额排名
create table ads_shop_rank(
    shop_id int,
    shop_gmv decimal(10,2),
    shop_rank int
);
insert overwrite table ads_shop_rank
select
    shop_id,
    sum(gmv) as shop_gmv,
    row_number() over (order by sum(gmv) desc) as shop_rank
from ods_sales_summary_daily
group by shop_id;
-- 属性分析指标
-- 属性流量占比：各属性带来的流量占总流量的比例
create table ads_source_share(
    source_type string,
    category_share decimal(10,2),
    category_pv int,
    category_avg decimal(10,2),
    category_pay_uv int,
    category_pv_share decimal(10,2)
);
insert overwrite table ads_source_share
select
    source_type,
    round(count(distinct category_id)/3,2) as category_share,
    sum(pv) as category_pv,
    avg(pv) as category_avg,
    sum(pay_uv) as category_pay_uv,
    round(sum(pv)/sum(sum(pv)) over(),2) as category_pv_share
from ods_traffic_source
group by source_type;
-- 各个阶段年龄占比
create table ads_age_share(
    age_range string,
    age_share decimal(10,2)
);
insert overwrite table ads_age_share
select
    age_range,
    round(count(distinct user_behavior)/sum(count(distinct user_behavior)) over(),2) as age_share
from ods_user_portrait
group by age_range;

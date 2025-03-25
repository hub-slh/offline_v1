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
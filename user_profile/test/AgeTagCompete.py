from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import pandas as pd

from pojo.tag_for_mate import TagForMate

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'


def parseTag4(rule_str:str):
    ru_dict = {}
    ru_list = rule_str.split(sep="##")
    for i in ru_list:
        tep_list = i.split(sep="=")
        ru_dict[tep_list[0]] = tep_list[1]
    return ru_dict


if __name__ == '__main__':
    # 定义需要用到的变量
    appName = "年龄标签计算"
    url = "jdbc:mysql://192.168.88.166:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    dbtable = "tbl_basic_tag"
    tag4Id = 14
    sql = f"select id, rule from {dbtable} where id = {tag4Id} or pid = {tag4Id}"
    result_index = 'tfec_userprofile_result'
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName(appName) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 开启arrow配置
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # 2 读取文件
    # 2.2 读取mysql文件

    tag_df = spark.read.format("jdbc")\
        .option("url",url)\
        .option("query",sql)\
        .option("user","root")\
        .option("password","123456")\
        .load()
    #                   format:文件格式
    '''
    完整如下：
        SparkSession对象.read.format('csv|json|text|parquet|jdbc|orc...')
                        .option('参数', '取值') 
                        # option('sep', ' ') 
                        # option('inferSchema', 'true') 
                        # option('schema', 'id string, name string, score int')
                        .schema(schema) # 设置表结构信息
                        .load(文件路径)
    '''
    # 查看读取结果
    # tag_df.show()

    # collect方法是将rdd中数据放入到一个list列表中
    # df按列处理
    # rdd按行处理
    rule_str = tag_df.rdd.map(lambda row:row['rule']).collect()[0]
    print(rule_str)

    # 生成规则字典
    rule_dict = parseTag4(rule_str)
    # print(rule_dict)
    # 创建rule对象
    tag4_mate = TagForMate.dict2TagMate(rule_dict)
    # print(tag4_mate)
    # ============================ 3.根据解析出来的rule读取ES数据 =========================
    es_df = spark.read.format("es")\
        .option('es.resource',tag4_mate.esIndex)\
        .option('es.nodes',tag4_mate.esNodes)\
        .option('es.read.field.include',tag4_mate.selectFields)\
        .load()

    # es_df.show()

    # ======================== 4.读取和年龄段标签相关的5级标签==============
    tag5_df = tag_df.where(f"id != {tag4Id}")
    tag5_df.show()
    # 5-1统一格式,将1999-09-09统一为:199909095
    birthday_df = es_df.select(es_df["id"].alias("userId"),F.regexp_replace("birthday","-","")[0:8].alias('birthday'))
    birthday_df.show()

    # 5-2将tag5_df拆分为("tagsId","start","end")
    tag5_split_df = tag5_df.select(tag5_df['id'].alias("tagsId"),
                                   F.split('rule', '-')[0].alias('start'),
                                   F.split('rule', '-')[1].alias('end'))
    tag5_split_df.show()


    # birthday_df和tag5_split_df5级标签join匹配

    result_df = birthday_df\
        .join(tag5_split_df,on=birthday_df['birthday'].between(tag5_split_df['start'],tag5_split_df['end']))\
        .select(birthday_df['userId'].cast(StringType()),tag5_split_df['tagsId'].cast(StringType()))

    result_df.show()
    # 6.将最终结果写到ES
    # es.write.operation=upsert 数据没有就插入，数据有就更新
    # es.mapping.id=userId 使用userId进行判断数据是否已存在，如果不配置，默认使用_id作为判断依据
    # es.mapping.name 如果df中的列名与es中的字段名不一样可以自定义设置，如果不设置默认根据df中的列名写入

    result_df.write.format('es')\
        .mode("append")\
        .option('es.resource',"result_index")\
        .option('es.nodes',f'{tag4_mate.esNodes}')\
        .option("es.mapping.id", "userId")\
        .option("es.write.operation", "upsert")\
        .save()


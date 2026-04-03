from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import pandas as pd
from pojo.tagformate import Tag4Mate
# 主要为了抽取基类



# - 2、读取标签的信息（四级和五级标签的id和rule）：逻辑一样   参数不一样-tag4Id
# - 3、解析四级标签的rule：逻辑一样 参数一样
# - 4、根据四级标签加载es中的数据：逻辑一样 参数一样
# - 5、读取计算用到五级标签信息 逻辑一样 参数一样
# - 6、标签的计算：逻辑不一样 参数一样
# - 7、标签的更新：逻辑一样 参数一样
# - 8、标签结果的写入：逻辑一样 参数一样

def parse4tag(rule_str:str):
    '''
    将字符串转化成字典
    :param rule_str: 字符串
    :return: 字典
    '''
    # 建立空字典备用
    ru_dict = {}
    # 将字符切割成列表
    tem_list = rule_str.split(sep="##")
    # 将列表遍历，对每一个元素进行再切割
    for i in tem_list:
        # 将每个字符切割成列表
        t2_list = i.split(sep="=")
        # 将每个列表第一个作为key,第二个作为value，添加到空列表中
        ru_dict[t2_list[0]] = t2_list[1]
    # 返回字典
    return ru_dict

@F.udf()
def tag_update(oid:str, nid:str, tag5id:str):
    '''
    将新标签列和旧标签列判断去重并拼接在一起
    :param oid:旧标签
    :param nid:新标签
    :param tag5id: 本机计算的所有5级标签
    :return:返回新的标签字符串
    '''
    # 将5级标签字符串切割成列表
    t5_list = tag5id.split(sep=',')
    # 将旧的标签id切割成列表
    oid_list = oid.split(sep=',')
    # 建立一个空列表备用
    new_list = []
    # 对旧标签id和本次计算的5级标签进行对比，在本次5级计算内的就去掉
    for i in oid_list:
        if i not in t5_list:
            new_list.append(i)
    # 将新的标签加入列表中
    new_list.append(str(nid))
    # 将新的列表用，拼接起来
    return ','.join(new_list)



class TagCompeleBase():
    # 定义需要用到的变量
    url = "jdbc:mysql://192.168.88.166:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    db_table = "tbl_basic_tag"
    out_index = "result_index"
    # 创建构造函数
    def __init__(self,tag4_id:str or int,app_name:str):
        self.tag4_id = tag4_id
        self.app_name = app_name

    # - 1、创建spark的运行环境：逻辑一样   参数不一样-appName
    def get_spark(self):
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .master("local[*]") \
            .config('spark.sql.shuffle.partitions', 2) \
            .getOrCreate()
        return spark

    # - 2、读取标签的信息（四级和五级标签的id和rule）：逻辑一样   参数不一样-tag4Id
    def read_meta(self):
        sql = f"select id, rule from {self.db_table} where id = {self.tag4_id} or pid = {self.tag4_id}"
        my_df = self.get_spark().read.format("jdbc") \
            .option("url", self.url) \
            .option("user", "root") \
            .option("password", "123456") \
            .option("query", sql) \
            .load()
        return my_df

    # 3.读取和性别标签相关的4级标签rule并解析得到类对象
    def get_rule_class(self):
        my_df = self.read_meta()
        # 3.1 取出rule
        tag4_rule_str = my_df.where(f'id = {self.tag4_id}').rdd.map(lambda row: row.rule).collect()[0]
        # 3.2 将rule转化为字典
        tag4_dict = parse4tag(tag4_rule_str)
        # 3.3 用字典建成一个类对象
        tag4mate = Tag4Mate.dict2tagmate(tag4_dict)
        # 返回一个类对象
        return tag4mate

    # 4.根据4级标签加载ES数据
    def get_es_df(self):
        spark = self.get_spark()
        tag4mate = self.get_rule_class()
        es_df = spark.read.format('es') \
            .option('es.resource', tag4mate.esIndex) \
            .option('es.nodes', tag4mate.esNodes) \
            .option('es.read.field.include', tag4mate.selectFields) \
            .load()
        return es_df

    # 5.读取和性别标签相关的5级标签(根据4级标签的id作为pid查询)
    def get_tag5_df(self):
        my_df = self.read_meta()
        tag5_df = my_df.where(f"id != {self.tag4_id}")
        return tag5_df

    # 6.根据ES数据和5级标签数据进行匹配,得出userId,tagsId
    def get_new_df(self):
        pass

    # 7.查询ES中的oldDF
    def get_old_es_df(self):
        spark = self.get_spark()
        tag4mate = self.get_rule_class()
        old_es_df = spark.read.format('es') \
            .option('es.resource', self.out_index) \
            .option('es.nodes', tag4mate.esNodes) \
            .load()
        return old_es_df

    # 8.合并newDF和oldDF
    def get_result_df(self):
        if self.get_old_es_df() is not None:
            tag5_df = self.get_tag5_df()
            new_df = self.get_new_df()
            old_es_df = self.get_old_es_df()
            # 8.1将五级标签所有id放在一个集合里
            tag5_list = tag5_df.select(tag5_df['id'].cast(StringType()).alias('id'))\
                .rdd.map(lambda row: row.id).collect()
            # 8.2 将五级标签的id集合增加到结果df中
            new1_df = new_df.withColumn('tag5id', F.lit(",".join(tag5_list)))
            # 8.3将新标签根据旧标签进行更新
            result_df =old_es_df.join(new1_df, on=old_es_df['userId'] == new1_df['userId'], how='left') \
                .select(old_es_df['userId'].cast(StringType()),
                        tag_update(old_es_df['tagsId'], new1_df['tagsId'], new1_df['tag5id']).alias('tagsId'))
        else:
            result_df = self.get_new_df()
        return result_df

    # 9.将最终结果写到ES
    def write_to_es(self):
        result_df = self.get_result_df()
        if result_df is not None:
            tag4mate = self.get_rule_class()
            result_df.write.format('es').mode('append')\
                .option('es.nodes', tag4mate.esNodes)\
                .option('es.resource',self.out_index)\
                .option('es.write.operation','upsert')\
                .option('es.mapping.id','userId')\
                .save()
            return print('成功写入es')
        else:
            print('result_df为空')


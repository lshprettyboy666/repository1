from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F
from pyspark.sql import Window as win

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'




if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('4级标签和5级标签读取') \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 开启arrow配置
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # 2 读取文件
    # 2.1 定义表结构     每个字段（名称，类型，可否为空）
    # schema1 = StructType() \
    #     .add('id', StringType(), True) \
    #     .add('name', StringType(), True) \
    #     .add('score', IntegerType(), True)


    def to_dict(x: str) -> dict:
        """
        将字符串转为字典
        :param x: 输入字符串
        :return: 返回字典
        """
        my_dict = {}
        rule_str = x.split(sep="##")
        for i in rule_str:
            list1 = i.split(sep="=")
            my_dict[list1[0]] = list1[1]
        return my_dict

    to_dict_dsl = spark.udf.register('to_dict_sql',to_dict,returnType=MapType(StringType(), StringType()))

    # 2.2 读取mysql文件
    url1 = "jdbc:mysql://192.168.88.166:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    df = spark.read.jdbc(url=url1,table="tbl_basic_tag")
    # 查看结果df
    df.show(truncate=False)

    df_mysql : DataFrame = df.select(df["rule"]).where('id = 4')
    df_mysql.show(truncate=False)
    # 四级标签结果
    df_mysql.printSchema()

    df_4rule = df_mysql.select(to_dict_dsl(df_mysql['rule']).alias('rule'))
    df_4rule.printSchema()

    df_5 = df_4rule.withColumn('rule',F.to_json(df_4rule['rule']))
    print("#"*20)
    df_5.printSchema()
    df_5.show()

    df_4rule.select(df_4rule['rule']['selectFields']).show(truncate=False)

    # 输出到mysql中
    # df.write.mode('overwrite').jdbc(
    #     url="jdbc:mysql://node1:3306/sparktest?useSSL=false&useUnicode=true&characterEncoding=utf8",
    #     table='stu2',
    #     properties={"user": "root", "password": "123456"})

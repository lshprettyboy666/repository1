from pyspark import SparkConf, SparkContext
import os

os.environ['JAVA_HOME'] = "/export/server/jdk1.8.0_241"


if __name__ == '__main__':
    # 1 创建spark运行环境
    conf = SparkConf().setAppName('wordcount').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    # 2 对数据进行处理
    # sc.textFile('D:/project/bigdata/zz17/day01/data/word.txt') # 错误路径
    # sc.textFile('/tmp/pycharm_project_956/day01/data/word.txt') # 如果路径中没有加任何协议，默认是到hdfs中读取
    # 2.1 读取数据
    # 如果路径中加协议file://，是到本地路径中读取
    rdd_file = sc.textFile('file:///tmp/pycharm_project_780/word.txt')
    # print(type(rdd_file))
    # 2.2 处理数据：分割数据，转成一个大列表
    rdd_flat = rdd_file.flatMap(lambda line:line.split())
    # ['kafka spark spark', 'hadoop hadoop flume']
    # [[kafka, spark, spark], [hadoop, hadoop, flume]]
    # [kafka, spark, spark, hadoop, hadoop, flume]
    # print(rdd_flat.collect())
    # 2.3 处理数据：把单词转成元组
    rdd_tuple = rdd_flat.map(lambda word:(word,1))
    # print(rdd_tuple.collect())
    # ('hello', 1), ('you', 1), ('Spark', 1), ('hello', 1), ('hello', 1)]
    # 3 词频统计
    result = rdd_tuple.reduceByKey(lambda agg,curr:curr+agg)
    # agg = 0
    # curr = 1
    # agg = curr+agg = 1
    # agg = 1
    # curr = 1
    # agg = agg + curr = 2
    # agg = 2
    # curr = 1
    # agg = agg + curr = 3
    # 4 调用action操作
    # action操作之前的操作称为transform操作，都是惰性
    print(result.collect())
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from base.tagcompelebase import TagCompeleBase
import numpy as np

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'


class TagComplete_Rfm(TagCompeleBase):
    # 重写get_new_df方法
    def get_new_df(self):
        tag5_df = self.get_tag5_df()
        es_df = self.get_es_df()
        # ===================================1、计算 R/F/M的值==========================================
        # R  1.1用户id分组消费时间求最大值, 最近的消费离今天有多久
        rv = F.datediff(F.date_sub(F.current_date(), 2063), F.from_unixtime(F.max(F.col('finishtime')), "yyyy-MM-dd")) \
            .alias("r")

        # F  1.2用户id分组 count计数 (去重计数) 总的消费次数
        fv = F.countDistinct('ordersn').alias("f")
        # M  1.3用户id分组 金额求和 总的消费金额
        mv = F.sum("orderamount").alias("m")
        # 1.4建立临时表
        tm_df: DataFrame = es_df.groupBy('memberid').agg(rv, fv, mv)
        # 1.5查看结果
        tm_df.describe().show()

        # 2、==========================================R/F/M 分别==打5分=========================================
        # 2.1
        rscore = F.when(F.col('r') == 7, 1) \
            .when(F.col('r') == 8, 2) \
            .when(F.col('r') == 9, 3).otherwise(4).alias("rscore")
        # 2.2
        fscore = F.when(F.col('f').between(250, 355), 1) \
            .when(F.col('f').between(200, 250), 2) \
            .when(F.col('f').between(8, 201), 3) \
            .when(F.col('f').between(8.3, 8.6), 4) \
            .otherwise(5).alias("fscore")
        # 2.3
        mscore = F.when(F.col('m') > 600000, 1) \
            .when(F.col('m').between(400001, 600000), 2) \
            .when(F.col('m').between(366299, 400000), 3) \
            .when(F.col('m').between(200000, 366299), 4) \
            .otherwise(5).alias("mscore")

        # es_df1 =es_df.withColumn("rscore",rscore).withColumn('fscore',fscore).withColumn('mscore',mscore)
        # 2.4
        es_df1 = tm_df.select('memberid', rscore, fscore, mscore)
        # 2.5查看结果
        es_df1.show()

        # ================================================3提取特征工程=====================================
        # 转变成向量df
        # 3.1定义向量器
        assembler = VectorAssembler(inputCols=['rscore', 'fscore', 'mscore'], outputCol='features')
        # 3.2查看结果
        print(assembler)
        # 3.3得到向量df
        features_df = assembler.transform(es_df1)
        # 3.4查看结果
        features_df.show()

        # ==============================================4训练模型============================================
        # 4.1定义Kmeans对象
        kmeans = KMeans(k=8,featuresCol='features', predictionCol='prediction',seed=7)
            # k设置的类别个数和
            # featuresCol设置特征列
            # predictionCol 设置目标列
            # seed 随机种子
        # 4.2建立模型
        model = kmeans.fit(features_df)
        # 4.3训练模型 得到结果
        result_df :DataFrame = model.transform(features_df)
        #4.4 查看结果
        result_df.show()


        # ============================================== 5、模型评估 ======================================
        #5.1 构建评估器
        evaluator = ClusteringEvaluator(predictionCol="prediction",featuresCol='features')
        # 5.2计算轮廓系数
        sc = evaluator.evaluate(result_df)
        print('轮廓系数',sc)

        # 5.3中心点信息
        centers = model.clusterCenters()
        print('中心点信息', centers)
        #5.4 求每个中心点得分总和
        centers_list = [np.sum(i) for i in centers]
        print(centers_list)
        #5.5 将中心点的得分和中心点所属于的类别存入到字典中
        cluster_dict = {}
        for i in range(0,len(centers_list)):
            cluster_dict[i] = centers_list[i]
        print(cluster_dict)

        # 5.6字典根据value值正序排序
        sort_dict = dict(sorted(cluster_dict.items(),key=lambda x:x[1],reverse=False))
        print(sort_dict)
        # 5.7取出五级标签的id
        tag5_list = tag5_df.rdd.map(lambda x:x.id).collect()
        print(tag5_list)
        rule_dict = dict(zip(sort_dict.keys(),tag5_list))
        print(rule_dict)
        # 5.8定义一个将kmeans聚类的标记和标签id转换的函数
        @F.udf()
        def to_id(pre):
            return rule_dict[pre]

        # ===================================6 得到新的用户标签表=======================================
        new_df = result_df.select(result_df['memberid'].alias('userId'),to_id('prediction').alias('tagsId'))

        # ===================================7 返回新的用户标签表=============================================
        return new_df


if __name__ == '__main__':
    # 创建类对象
    tag = TagComplete_Rfm(69, "计算RFM标签")
    # 查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df().show()

    # 执行写入es
    tag.write_to_es()

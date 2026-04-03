from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import pandas as pd
from base.tagcompelebase import TagCompeleBase
import numpy as np

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

# 创建子类，继承父类
class Tagcompleterfm(TagCompeleBase):
    # 复写get_new_df
    def get_new_df(self):
        # 获得5级标签tag5_df
        tag5_df = self.get_tag5_df()
        # 获得原始数据es_df
        es_df = self.get_es_df()
        # ===================================1、计算 R/F/M的值==========================================
        # R  1.1用户id分组消费时间求最大值, 最近的消费离今天有多久
        r = F.datediff(F.date_sub(F.current_date(),2063),
                       F.from_unixtime(F.max('finishtime'),"yyyy-MM-dd")).alias('r1')
        # F  1.2用户id分组 count计数 (去重计数) 总的消费次数
        f = F.countDistinct("ordersn").alias("f1")

        # M  1.3用户id分组 金额求和 总的消费金额
        m = F.sum("orderamount").alias("m1")
        # 1.4建立临时表
        tm_df = es_df.groupBy('memberid').agg(r,f,m)
        # 1.5查看结果
        tm_df.show()
        # 1.6查看各个统计值
        tm_df.describe().show()
        # 2、==========================================R/F/M 分别==打5分=========================================
        # 2.1 R打分
        rscore = F.when(F.col('r1').between(1, 7), 5) \
            .when(F.col('r1').between(8, 14), 4) \
            .when(F.col('r1').between(15, 30), 3) \
            .when(F.col('r1').between(31, 60), 2) \
            .otherwise(1).alias('rscore')
        # 2.2 F打分
        fscore = F.when(F.col('f1').between(1, 100), 1) \
            .when(F.col('f1').between(101, 150), 2) \
            .when(F.col('f1').between(151, 200), 3) \
            .when(F.col('f1').between(201, 300), 4) \
            .otherwise(5).alias('fscore')
        # 2.3 M打分
        mscore = F.when(F.col('m1').between(1, 200000), 1) \
            .when(F.col('m1').between(200001, 300000), 2) \
            .when(F.col('m1').between(300001, 500000), 3) \
            .when(F.col('m1').between(500001, 800000), 4) \
            .otherwise(5).alias('mscore')
        # 2.4 得到打分结果sc_df
        sc_df = tm_df.select(F.col('memberid').alias('userId'),rscore,fscore,mscore)
        # 2.5查看结果
        sc_df.show()
        # ================================================3提取特征工程=====================================
        # 转变成向量df
        # 3.1定义向量器
        assembler = VectorAssembler(inputCols=['rscore','fscore','mscore'],outputCol='features')
        # 3.2得到向量df
        ve_df = assembler.transform(sc_df)
        # 3.3 查看结果
        ve_df.show()
        # ==============================================4训练模型============================================
        # 4.1定义Kmeans对象
        kmeans = KMeans(featuresCol="features",predictionCol="prediction",k=7,seed=12)
        # 4.2建立模型
        model = kmeans.fit(ve_df)
        # 4.3训练模型 得到结果
        km_df :DataFrame = model.transform(ve_df)
        # 4.4 查看结果
        km_df.show()
        # ============================================== 5、模型评估 ======================================
        #5.1 构建评估器
        evalutor = ClusteringEvaluator(predictionCol="prediction",featuresCol='features')
        # 5.2计算轮廓系数
        sc = evalutor.evaluate(km_df)
        # 5.3 查看sc 在[-1,1]之间，越大模型越好
        print('轮廓系数', sc)
        # ==============================6、获取模型中心点形成对应分数，并和标签表做好对应==================================
        # 6.1取出中心点信息。结果是个大列表
        centers = model.clusterCenters()
        # 6.2查看中心点
        print('中心点信息', centers)
        # 6.3将每个中心点求和，形成列表
        centers_list = [sum(i) for i in centers]
        # 6.4 查看centers_list
        print(f"centers_list:{centers_list}")
        # 6.5 将中心点的得分和中心点所属于的类别存入到字典中
        # 6.5.1 空字典备用
        score_dict = {}
        # 6.5.2将中心分数和与km打的标记对应做成字典{标记值：中心分数和}
        for i in range(0,len(centers_list)):
            score_dict[f'{i}'] = centers_list[i]
        # 6.5.3 查看score_dict
        print(score_dict)
        # 6.5.4 根据分析，中心和越大对应标签id越大，所以这里要将score_dict按降序排，来对应5级标签的的顺序
        sort_dict = dict(sorted(score_dict.items(),key=lambda x:x[1],reverse=True))
        # 6.5.5 查看sort_dict
        print(sort_dict)
        # 6.5.6 取得5级标签的id列表
        tag5_list = tag5_df.rdd.map(lambda x:x.id).collect()
        # 6.5.7 查看tag5_list
        print(tag5_list)
        # 6.5.8 通过sort_dict和tag5_list将km打的标记和5级标签关联成一个新字典
        id_prediction_dict =dict(zip(sort_dict.keys(),tag5_list))
        # 6.5.8 查看id_prediction_dict
        print(id_prediction_dict)

        # 6.5.9 定义udf函数，利用字典换将km标记转化为5级标签id
        @F.udf()
        def pre_to_tagid(n):
            return id_prediction_dict[f"{n}"]

        # =============================7、给km_df打上5级id标签===========================
        new_df = km_df.select('userId',pre_to_tagid("prediction").alias("tagsId"))
        return new_df








if __name__ == '__main__':
    # 创建类对象
    tag = Tagcompleterfm(38,"rfm客户价值计算")
    # 调用get_es_df函数查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df()

    # 写入es
    tag.write_to_es()
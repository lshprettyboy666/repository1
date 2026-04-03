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
class TagCompletePsm(TagCompeleBase):
    # 复写get_new_df
    def get_new_df(self):
        # 获得5级标签tag5_df
        tag5_df = self.get_tag5_df()
        # 获得原始数据es_df
        es_df = self.get_es_df()
        # ===================================1、计算 psm 的值==========================================
        # 1.1优惠金额
        da = F.col('couponcodevalue').alias('da')
        # 实收金额
        pa = F.col('orderamount').alias('pa')
        # 应收金额
        ra = da + pa

        # 1.4建立临时表
        tm_df = es_df.select(ra.alias('ra'), da, pa, F.col('memberid').alias('userId'), F.col('ordersn'))
        # 1.5查看结果
        print("==========查看tm_df=========")
        tm_df.show()
        # 1.6查看各个统计值
        print("==========查看tm_df统计=========")
        tm_df.describe().show()

        # 增加一列是否优惠识别列得到新表rdp_new_df
        rdp_new_df = tm_df.withColumn('isCoupon', F.when(F.col('da') == 0, 0).otherwise(1))
        # 查看rdp_new_df
        print("==========查看rdp_new_df=========")
        rdp_new_df.show()
        # 优惠订单数
        tdon = F.sum(F.col('isCoupon')).alias('tdon')
        # 总订单总数
        ton = F.count(F.col('ordersn')).alias('ton')
        # 平均优惠金额
        ada = F.avg(F.col('da')).alias('ada')
        # 平均每单应收
        ara = F.avg(F.col('ra')).alias('ara')
        # 优惠总金额
        tda = F.sum(F.col('da')).alias('tda')
        # 应收总金额
        tra = F.sum(F.col('ra')).alias('tra')
        # 得到聚合表es_agg_df
        es_agg_df = rdp_new_df.groupBy('userId').agg(tdon,ton,ada,ara,tda,tra)
        print("==========查看es_agg_df=========")
        es_agg_df.show()

        # 优惠订单占比
        tdonr = (F.col('tdon') / F.col('ton')).alias('tdonr')
        # 平均优惠金额占比
        adar = (F.col('ada') / F.col('ara')).alias('adar')
        # 优惠总金额占比
        tdar = (F.col('tda') / F.col('tra')).alias('tdar')

        psm_df = es_agg_df.select('userId', tdonr, adar, tdar)
        print("==========查看psm_df=========")
        psm_df.show()
        # 3个占比合并得到新表psm_score_df
        psm_score_df = psm_df.select('userId', (F.col('tdonr') + F.col('adar') + F.col('tdar')).alias('psm'))
        print("==========查看psm_score_df=========")
        psm_score_df.show()





        # ================================================3提取特征工程=====================================
        # 转变成向量df
        # 3.1定义向量器
        assembler = VectorAssembler(inputCols=['psm'],outputCol='features')
        # 3.2得到向量df
        ve_df = assembler.transform(psm_score_df)
        # 3.3 查看结果
        print("==========查看ve_df=========")
        ve_df.show()
        # ==============================================4训练模型============================================
        # 4.1定义Kmeans对象
        kmeans = KMeans(featuresCol="features",predictionCol="prediction",k=5,seed=12)
        # 4.2建立模型
        model = kmeans.fit(ve_df)
        # 4.3训练模型 得到结果
        km_df :DataFrame = model.transform(ve_df)
        # 4.4 查看结果
        print("==========查看km_df=========")
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
        # # 6.3将每个中心点求和，形成列表
        centers_list = [sum(i) for i in centers]
        # 6.4 查看centers_list
        print(f"查看centers_list:{centers_list}")
        # 6.5 将中心点的得分和中心点所属于的类别存入到字典中
        # 6.5.1 空字典备用
        score_dict = {}
        # 6.5.2将中心分数和与km打的标记对应做成字典{标记值：中心分数和}
        for i in range(0,len(centers_list)):
            score_dict[f'{i}'] = centers_list[i]
        # 6.5.3 查看score_dict
        print("==========查看score_dict=========")
        print(score_dict)
        # # 6.5.4 根据分析，中心越大对应标签id越大，所以这里要将score_dict按降序排，来对应5级标签的的顺序
        sort_dict = dict(sorted(score_dict.items(),key=lambda x:x[1],reverse=True))
        # 6.5.5 查看sort_dict
        print("==========查看ssort_dict=========")
        print(sort_dict)
        # 6.5.6 取得5级标签的id列表
        tag5_list = tag5_df.rdd.map(lambda x:x.id).collect()
        # 6.5.7 查看tag5_list
        print("==========查看tag5_list=========")
        print(tag5_list)
        # 6.5.8 通过sort_dict和tag5_list将km打的标记和5级标签关联成一个新字典,这里直接通过中心分数和于5级标签内容进行比对，得出对应关系
        id_prediction_dict =dict(zip(sort_dict.keys(),tag5_list))
        # 6.5.8 查看id_prediction_dict
        print("==========查看id_prediction_dict=========")
        print(id_prediction_dict)

        # 6.5.9 定义udf函数，利用字典换将km标记转化为5级标签id
        @F.udf()
        def pre_to_tagid(n):
            '''
            通过km标记作为key取得对应的value,就是5级标签id
            :param n:
            :return:
            '''
            return id_prediction_dict[f"{n}"]
        #
        # # =============================7、给km_df打上5级id标签===========================
        new_df = km_df.select('userId',pre_to_tagid("prediction").alias("tagsId"))
        return new_df








if __name__ == '__main__':
    # 创建类对象
    tag = TagCompletePsm(51,"psm价格敏感度标签计算")
    # 调用get_es_df函数查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df().show()

    # 写入es
    tag.write_to_es()
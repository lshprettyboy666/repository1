from pyspark.sql import DataFrame

from base.tagcompelebase import TagCompeleBase
import pyspark.sql.functions as F
# 创建子类重写处理方法get_new_df
class TagComplete_Consumer(TagCompeleBase):
    # 重写get_new_df方法
    def get_new_df(self):
        "inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,politicalface"
        # 得到原始es_df
        es_df :DataFrame = self.get_es_df()
        # 将es_df根据id分组求最近消费的时间
        es_df_grou = es_df.groupBy("memberid").agg(F.max("finishtime").alias("finishtime"))
        es_df_format = es_df_grou.select("memberid",F.from_unixtime( es_df_grou.finishtime,'yyyy-MM-dd').alias("finishtime"))
        es_df_fi = es_df_format.withColumn('diff',F.datediff(F.date_sub(F.current_date(),2050),es_df_format.finishtime))
        # 得到原始5级标签表
        tag5_df:DataFrame = self.get_tag5_df()
        # 将tag5_df的 rule拆成2列，分别为start1,end1
        tag5_df_fi = tag5_df.select('id',F.split("rule","-")[0].alias("start1"),F.split("rule","-")[1].alias("end1"))
        # 联查得到new_df
        new_df = es_df_fi.join(tag5_df_fi, on=es_df_fi.diff.between(tag5_df_fi.start1,tag5_df_fi.end1)
                            , how='left_outer') \
            .select(es_df_fi.memberid.alias('userId'), tag5_df.id.alias('tagsId'))
        return new_df


if __name__ == '__main__':
    # 创建类对象
    tag = TagComplete_Consumer(23, "计算消费周期标签")
    # 查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df().show()

    # 执行写入es
    tag.write_to_es()

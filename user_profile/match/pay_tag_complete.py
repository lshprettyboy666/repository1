from pyspark.sql import DataFrame
from pyspark.sql import Window as win



from base.tagcompelebase import TagCompeleBase
import pyspark.sql.functions as F
# 创建子类重写处理方法get_new_df
class TagComplete_Pay(TagCompeleBase):
    # 重写get_new_df方法
    def get_new_df(self):
        "inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_orders##esType=_doc##selectFields=memberid,paymentcode"
        # 得到原始es_df
        es_df :DataFrame = self.get_es_df()
        es_df_group :DataFrame = es_df.groupBy(es_df.memberid,es_df.paymentcode).agg(F.count(es_df['paymentcode']).alias('total'))
        es_df_rk = es_df_group.select('memberid','paymentcode',
                                      F.row_number().over(win.partitionBy('memberid').orderBy(es_df_group['total'].desc())).alias('rk'))
        es_df_fi = es_df_rk.where('rk = 1')
        # 得到原始5级标签表
        tag5_df:DataFrame = self.get_tag5_df()

        # 联查得到new_df
        new_df = es_df_fi.join(tag5_df, on=es_df_fi['paymentcode'] == tag5_df['rule']
                            , how='left_outer') \
            .select(es_df_fi.memberid.alias('userId'), tag5_df.id.alias('tagsId'))
        return new_df


if __name__ == '__main__':
    # 创建类对象
    tag = TagComplete_Pay(30, "计算支付方式标签")
    # 查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df().show()

    # 执行写入es
    tag.write_to_es()

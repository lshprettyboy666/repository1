from base.tagcompelebase import TagCompeleBase


# 创建新类
class TagComplete_Polit(TagCompeleBase):
    # 重写get_new_df方法
    def get_new_df(self):
        "inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,politicalface"
        es_df = self.get_es_df()
        tag5_df = self.get_tag5_df()
        new_df = es_df.join(tag5_df, on=es_df.politicalface == tag5_df.rule
                            , how='left_outer') \
            .select(es_df.id.alias('userId'), tag5_df.id.alias('tagsId'))
        return new_df


if __name__ == '__main__':
    # 创建类对象
    tag = TagComplete_Polit(62, "计算政治面貌标签")
    # 查看es_df
    tag.get_es_df().show()
    # 查看tag5_df
    tag.get_tag5_df().show()
    # 查看new_df
    tag.get_new_df().show()

    # 执行写入es
    tag.write_to_es()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


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


class Tag4Mate():
    '''
    建议解析rule的对象
    '''
    def __init__(self,inType, esNodes, esIndex,esType,selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esIndex = esIndex
        self.esType = esType
        self.selectFields = selectFields
    def dict2tagmate(tag4_dict:dict):
        '''
        调用类
        :return: 返回类对象
        '''
        return Tag4Mate(inType=tag4_dict.get("inType"),
                        esNodes=tag4_dict.get("esNodes"),
                        esIndex=tag4_dict.get("esIndex"),
                        esType=tag4_dict.get("esType"),
                        selectFields=tag4_dict.get("selectFields")
                        )

# 注册udf函数
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

if __name__ == '__main__':
    # 定义需要用到的变量
    app_name = "性别标签计算"
    url = "jdbc:mysql://192.168.88.166:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    db_table = "tbl_basic_tag"
    out_index = "result_index"
    tag4_id = 4
    sql = f"select id, rule from {db_table} where id = {tag4_id} or pid = {tag4_id}"

    # 1.准备Spark开发环境
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config('spark.sql.shuffle.partitions', 2) \
        .getOrCreate()
    # 2.读取MySQL数据
    my_df = spark.read.format("jdbc") \
        .option("url",url) \
        .option("user","root") \
        .option("password","123456") \
        .option("query",sql) \
        .load()
    # 查看结果
    my_df.show()

    # 3.读取和性别标签相关的4级标签rule并解析
    #3.1 取出rule
    tag4_rule_str = my_df.where('id = 4').rdd.map(lambda row:row.rule).collect()[0]
    # 查看结果
    print(tag4_rule_str)
    # 3.2 将rule转化为字典
    tag4_dict = parse4tag(tag4_rule_str)
    # 查看结果
    print(tag4_dict)
    # 3.3 用字典建成一个类对象
    tag4mate = Tag4Mate.dict2tagmate(tag4_dict)
    # 查看结果
    print(tag4mate)
    # 4.根据4级标签加载ES数据
    es_df = spark.read.format('es')\
        .option('es.resource',tag4mate.esIndex)\
        .option('es.nodes',tag4mate.esNodes)\
        .option('es.read.field.include',tag4mate.selectFields)\
        .load()
    # 查看结果
    es_df.show()
    # 5.读取和性别标签相关的5级标签(根据4级标签的id作为pid查询)
    tag5_df = my_df.where(f"id != {tag4_id}")
    # 查看结果
    tag5_df.show()

    # 6.根据ES数据和5级标签数据进行匹配,得出userId,tagsId
    new_df = es_df.join(tag5_df, on=es_df.gender == tag5_df.rule, how="left")\
        .select(es_df['id'].alias('userId'),tag5_df['id'].alias('tagsId'))
    # 查看结果
    new_df.show()

    # 7.查询ES中的oldDF
    old_es_df = spark.read.format('es')\
        .option('es.resource',out_index)\
        .option('es.nodes',tag4mate.esNodes)\
        .load()
    # 查看结果
    old_es_df.show()

    # 8.合并newDF和oldDF
    # 8.1将五级标签所有id放在一个集合里
    tag5_list = tag5_df.select(tag5_df['id'].cast(StringType()).alias('id')).rdd.map(lambda row:row.id).collect()
    # 查看结果
    print(tag5_list)
    #8.2 将五级标签的id集合增加到结果df中
    new1_df = new_df.withColumn('tag5id',F.lit(",".join(tag5_list)))
    # 查看结果
    new1_df.show()
    # 8.3将新标签根据旧标签进行更新
    result_df = old_es_df.join(new1_df,on=old_es_df['userId'] == new1_df['userId'],how='left')\
        .select(old_es_df['userId'].cast(StringType()),tag_update(old_es_df['tagsId'],new1_df['tagsId'],new1_df['tag5id']).alias('tagsId'))
    # 结果查看
    result_df.show()

    # 9.将最终结果写到ES
    result_df.write.format('es').mode('append')\
        .option('es.nodes',tag4mate.esNodes)\
        .option('es.resource',out_index)\
        .option('es.write.operation','upsert')\
        .option('es.mapping.id','userId')\
        .save()

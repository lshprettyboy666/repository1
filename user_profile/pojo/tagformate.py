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
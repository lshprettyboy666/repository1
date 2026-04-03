class TagForMate():
    def __init__(self, inType,esNodes,esIndex,esType,selectFields):
        self.inType: str  = inType
        self.esNodes: str = esNodes
        self.esIndex: str = esIndex
        self.esType: str = esType
        self.selectFields: str = selectFields

    def dict2TagMate(rule_dict:dict):
        return TagForMate(inType = rule_dict.get("inType"),
                          esNodes= rule_dict.get("esNodes"),
                          esIndex = rule_dict.get('esIndex'),
                          esType=rule_dict.get('esType'),
                          selectFields=rule_dict.get('selectFields'))
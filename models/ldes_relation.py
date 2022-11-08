
class LdesRelation():

    def __init__(
        self,
        source_node: str, 
        relation_type: str, 
        target_loaction: str, 
        target_node_uri: str,
        is_processed: bool,
    ):
        self.source_node = source_node
        self.relation_type = relation_type
        self.target_location = target_loaction
        self.target_node_uri = target_node_uri
        self.is_processed = is_processed
    
    def to_tuple(self):
        return (
            self.source_node, 
            self.relation_type,
            self.target_location, 
            self.target_node_uri,
            self.is_processed)
    
    def __str__(self):
        return f"""<LdesRelation source_node='{self.source_node}' \
relation_type='{self.relation_type}' \
tagret_location='{self.target_location}' \
target_node_uri='{self.target_node_uri}' \
is_processed={self.is_processed}>"""
import datetime

class LdesNode():

    def __init__(self, node_uri: str, node_location: str, view_uri: str, node_payload:str, etag: str=None, expires: datetime.datetime=None, immutable = False):
        self.uri = node_uri
        self.view_uri = view_uri
        self.location = node_location
        self.payload = node_payload
        self.etag = etag
        self.expires = expires
        self.immutable = immutable
    
    def to_tuple(self):
        return (self.uri, self.location, self.view_uri, self.payload, self.etag, self.expires, self.immutable)

    def __str__(self):
        return f"""<LdesNode \
uri='{self.uri}' \
view_uri='{self.view_uri}' \
location='{self.location}' \
etag='{self.etag}' \
immutable={self.immutable}>"""
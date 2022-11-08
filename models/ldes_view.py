
class LdesView():

    def __init__(self,  view_uri: str, view_location: str, view_alias: str, polling: int = 60, sync: bool = False):
        self.uri = view_uri
        self.location = view_location
        self.alias = view_alias
        self.polling = polling
        self.sync = sync
        
    def to_tuple(self):
        return (self.uri, self.location, self.alias, self.polling, self.sync)

    def __str__(self):
        return f"<LdesView \
uri='{self.uri}' \
location='{self.location}' \
alias='{self.alias}' \
polling={self.polling} \
sync={self.sync}>"

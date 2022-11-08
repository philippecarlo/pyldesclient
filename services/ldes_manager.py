'''
LDES MANAGER
Concerns of the LDES manager:
 - Adding new LDES streams
 - Updating existing LDES streams
 - Deleting LDES streams
 - Reporting LDES stream status
'''
from sqlite3 import Error
from services.ldes_store import LdesStore
from services.ldes_client import LdesClient


class LdesManager():
    
    def __init__(self, alias: str, ldes_store: LdesStore, ldes_client: LdesClient):
        self.alias = alias
        self.ldes_store = ldes_store
        self.ldes_client = ldes_client

    def onboard_ldes_view(self, alias: str, location: str, polling: int = 60):
        self.ldes_store.create_view_db()
        the_view  = self.ldes_client.get_ldes_view(location)
        the_view.polling = polling
        self.ldes_store.create_view(the_view)

    def update_ldes_view(self, alias: str, location: str, polling: int = 60):
        pass

    def delete_ldes_view(self):
        self.ldes_store.delete_view_db()

    def report_ldes_view_status(self, alias: str):
        pass


'''
LDES SYNCER
Concerns of the LDES syncer:
 - Synchronizing an LDES stream by regularly polling known mutable LDES nodes
 - [optional extension] Synchronizing an LDES stream by subscribing to a web socket endpoint/Kafka topic/...
'''
import schedule
import time
from typing import List
from models import LdesView, LdesNode, LdesRelation
from ldes_client_error import LdesClientError
from services.ldes_store import LdesStore
from services.ldes_client import LdesClient


class LdesSyncer():
    
    def __init__(self, alias: str, ldes_store: LdesStore, ldes_client: LdesClient):
        self.alias = alias
        self.view = None
        self.ldes_store = ldes_store
        self.ldes_client = ldes_client
        self.do_sync=False

    def __do_sync(self):
        print (f"Polling LDES view {self.alias}")
        ## (1) always poll the root node (view or subset) and add the relations it contains
        is_changed, view_node, relations = self.ldes_client.get_ldes_node(self.view.location, None)
        if is_changed:
            self.handle_node(view_node, relations)

        ## (2) poll the any immutable nodes 
        mutable_nodes = self.ldes_store.get_nodes(mutable_only=True)
        for node in mutable_nodes:
            is_changed, the_node, relations = self.ldes_client.get_ldes_node(node.location, node.etag)
            if is_changed:
                print(f"Processing changed node  {node.uri} at location {node.location}")
                self.handle_node(the_node, relations)
            else:
                print(f"Skipping unmodified node {node.uri} at location {node.location} and marking as immutable")
                # IMPORTANT REMARK: marking as immutable here assuming 304 means that the fragment IS immutable from now on...
                node.immutable = True
                self.ldes_store.update_node(node)

        ## (3) process any unprocessed relations by fetching their target nodes and adding their relations in turn
        relations = self.ldes_store.get_relations(unprocessed_only=True)
        for rel in relations:
            print(f"Processing {rel.relation_type} link to {rel.target_location}")
            self.handle_relation(rel)
        
    def sync(self):
        ## check if a view with the existing alias exists
        self.view = self.ldes_store.get_view()
        self.view.sync = True
        self.ldes_store.update_view(self.view)
        if not self.view:
            raise LdesClientError(f'LDES Syncing Error: view alias {self.alias} does not exist. Check the alias or onboard the LDES view first.')
        schedule.every(self.view.polling).seconds.do(self.__do_sync)
        
        while self.view.sync:
            schedule.run_pending()
            time.sleep(1)
            # check for stop signal ...
            self.view = self.ldes_store.get_view() 

    def handle_node(self, ldes_node: LdesNode, relations: List[LdesRelation]):
        ## check if the node is already stored
        the_node = self.ldes_store.get_node(ldes_node.uri)
        if not the_node:
            self.ldes_store.create_node(ldes_node)
        elif not the_node.immutable:
            self.ldes_store.update_node(ldes_node)
        
        for relation in relations:
            ## check if the relation is already stored
            the_relation = self.ldes_store.get_relation(relation.source_node, relation.relation_type)
            if not the_relation:
                self.ldes_store.create_relation(relation)

    
    def handle_relation(self, rel: LdesRelation):
        the_node = self.ldes_store.get_node(rel.target_location)
        etag = the_node.etag if the_node else None
        is_changed, node, relations = self.ldes_client.get_ldes_node(rel.target_location, etag)
        if is_changed:
            self.handle_node(node, relations)
        rel.is_processed = True
        self.ldes_store.update_relation(rel)
    
    def stop_sync(self):
        self.view = self.ldes_store.get_view()
        self.view.sync = False
        self.ldes_store.update_view(self.view)

import sqlite3
from sqlite3 import Error
import os
from pathlib import Path
from typing import List

from models import LdesNode, LdesView, LdesRelation
from ldes_client_error import LdesClientError

VIEWS_TABLE= \
"""
CREATE TABLE IF NOT EXISTS views (
    uri VARCHAR(2048) PRIMARY KEY, 
    location VARCHAR(2048) NOT NULL,
    alias VARCHAR(100) NOT NULL UNIQUE,
    polling INT DEFAULT 60 NOT NULL,
    sync INT DEFAULT 0 NOT NULL
);"""

NODES_TABLE= \
"""
CREATE TABLE IF NOT EXISTS nodes (
    uri VARCHAR(2048) PRIMARY KEY,
    view_uri VARCHAR(2048) NOT NULL,
    location VARCHAR(2048) NOT NULL,
    payload TEXT,
    etag VARCHAR(50),
    expires DATETIME,
    immutable INT DEFAULT 0 NOT NULL,
    FOREIGN KEY (view_uri) REFERENCES views(uri)
)
"""

RELATIONS_TABLE= \
"""
CREATE TABLE IF NOT EXISTS relations (
    source_node NOT NULL,
    relation_type NOT NULL,
    target_location VARCHAR(2048) NOT NULL,
    target_node_uri VARCHAR(2048),
    is_processed  INT DEFAULT 0 NOT NULL,
    FOREIGN KEY (target_node_uri) REFERENCES nodes(uri)
    PRIMARY KEY (source_node, relation_type)
)
"""

class LdesStore():

    def __init__(self, location: str, alias: str):
        self.alias = alias
        self.location = location
        self.connection_string = f'{self.location}/{alias}.db'

    def get_connection(self):
        conn = sqlite3.connect(self.connection_string)
        return conn

    def create_view_db(self):
        if not os.path.exists(self.connection_string):
            if not os.path.exists(self.location):
                os.makedirs(self.location)

            conn = None
            try:
                conn = sqlite3.connect(self.connection_string)
                cursor =  conn.cursor()
                cursor.execute(VIEWS_TABLE)
                cursor.execute(NODES_TABLE)
                cursor.execute(RELATIONS_TABLE)
            except Error as e:
                raise LdesClientError("Could not create view database.", e)
            finally:
                if conn:
                    conn.close()
        else:
            raise LdesClientError(f'The collection database {self.connection_string} already exists.')

    def delete_view_db(self):
        if not os.path.exists(self.connection_string):
            raise LdesClientError(f'The collection database {self.connection_string} does not exist.')
        else:
            os.remove(self.connection_string)

    #region *** VIEW Functions ***
    def create_view(self, view: LdesView):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM views')
            data=cursor.fetchall()
            if len(data) == 0:
                cursor.execute('INSERT INTO views(uri, location, alias, polling, sync) VALUES (?, ?, ?, ?, ?)', view.to_tuple())
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def update_view(self, view: LdesView):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''UPDATE views SET polling=?,sync=? WHERE uri=?''', (view.polling, view.sync, view.uri))
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def get_view(self) -> LdesView:
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT uri, location, alias, polling, sync FROM views')
            uri, location, alias, polling, sync = cursor.fetchone()
            the_view = LdesView(uri, location, alias, polling, sync)
            return the_view
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def delete_view(self, alias):
        raise LdesClientError("Deleting a view is not supported.")
    
    #endregion

    #region *** NODE Functions ***
    def create_node(self, node: LdesNode):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO nodes(uri, location, view_uri, payload, etag, expires, immutable) VALUES (?,?,?,?,?, ?,?)', node.to_tuple())
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def update_node(self, node: LdesNode):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''UPDATE nodes SET payload=?,etag=?,expires=?,immutable=? WHERE uri=?''', (node.payload, node.etag, node.expires, node.immutable, node.uri))
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def get_nodes(self, mutable_only: bool = False) -> List[LdesNode]:
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            result = []
            if mutable_only:
                cursor.execute('SELECT uri, location, view_uri, payload, etag, expires, immutable FROM nodes WHERE immutable=0')
                records = cursor.fetchall()
                for row in records:
                    uri, location, view_uri, payload, etag, expires, immutable = row
                    result.append(LdesNode(uri, location, view_uri, payload, etag, expires, immutable))
            else:
                cursor.execute('SELECT uri, location, view_uri, payload, etag, expires, immutable FROM nodes')
                records = cursor.fetchall()
                for row in records:
                    uri, location, view_uri, payload, etag, expires, immutable = row
                    result.append(LdesNode(uri, location, view_uri, payload, etag, expires, immutable))
            return result
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def get_node(self, node_uri: str) -> LdesNode:
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT uri, location, view_uri, payload, etag, expires, immutable FROM nodes WHERE uri=?', (node_uri,))
            records = cursor.fetchall()
            if len(records) > 0:
                uri, location, view_uri, payload, etag, expires, immutable = records[0]
                the_node = LdesNode(uri, location, view_uri, payload, etag, expires, immutable)
                return the_node
            else:
                return None
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    #endregion

    #region *** RELATION Functions ***
    def get_relation(self, node_uri: str, relation_type: str):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM relations WHERE source_node=? AND relation_type=?', (node_uri,relation_type,))
            records = cursor.fetchall()
            if len(records) > 0:
                source_node, relation_type, target_location, target_node_uri, is_processed = records[0]
                the_relation = LdesRelation(source_node, relation_type, target_location, target_node_uri, is_processed)
                return the_relation
            else:
                return None
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()
        

    def get_relations(self, unprocessed_only=False) -> List[LdesRelation]:
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            result = []
            if unprocessed_only:
                cursor.execute('SELECT source_node, relation_type, target_location, target_node_uri, is_processed FROM relations WHERE is_processed=0')
                records = cursor.fetchall()
                for row in records:
                    source_node, relation_type, target_location, target_node_uri, is_processed = row
                    result.append(LdesRelation(source_node, relation_type, target_location, target_node_uri, is_processed))
            else:
                cursor.execute('SELECT source_node, relation_type, target_location, target_node_uri, is_processed FROM nodes')
                records = cursor.fetchall()
                for row in records:
                    source_node, relation_type, target_location, target_node_uri, is_processed = row
                    result.append(LdesRelation(source_node, relation_type, target_location, target_node_uri, is_processed))
            return result
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()


    def get_node_relations(self, node_uri: str) -> List[LdesRelation]:
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            result = []
            cursor.execute('SELECT source_node, relation_type, target_location, target_node_uri, is_processed FROM relations WHERE source_node=?', (node_uri,))
            records = cursor.fetchall()
            for row in records:
                source_node, relation_type, target_location, target_node_uri, is_processed = row
                result.append(LdesRelation(source_node, relation_type, target_location, target_node_uri, is_processed))
            return result
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def create_relation(self, rel: LdesRelation):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO relations(source_node, relation_type, target_location, target_node_uri, is_processed) VALUES (?,?,?,?,?)', rel.to_tuple())
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    def update_relation(self, rel: LdesRelation):
        conn = None
        try: 
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''UPDATE relations SET is_processed=? WHERE source_node=? AND relation_type=?''', (rel.is_processed, rel.source_node, rel.relation_type, ))
            conn.commit()
        except Error as err:
            raise LdesClientError(err)
        finally:
            conn.close()

    #endregion


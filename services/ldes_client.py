import http.client
from typing import List, Tuple
from urllib.parse import urlparse
from models import LdesNode, LdesView
from ldes_client_error import LdesClientError

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import DCAT, DCTERMS, DCMITYPE, SOSA
from namespace import LDES, TREE, PYLDES
from models  import LdesRelation


class LdesClient():

    def __init__(self):
        pass

    def get_ldes_view(self, location: str) -> LdesView:
        try:
            url = urlparse(location)
            conn = http.client.HTTPConnection(url.hostname, url.port)
            headers = {'Accept': 'text/turtle'}
            conn.request('GET', url.path, headers=headers)
            response = conn.getresponse().read().decode()
            g = Graph()
            g.parse(data=response, format='turtle')
            return self.rdf_to_view(location, g)
        except Exception as error:
            raise LdesClientError(f"Failed to get the view from {location}. {error}", error)

    def get_ldes_node(self, location: str, etag: str = None) -> Tuple[bool, LdesNode, List[LdesRelation]]:
        try:
            url = urlparse(location)
            conn = http.client.HTTPConnection(url.hostname, url.port)
            headers = {'Accept': 'text/turtle'}
            if etag: headers['If-None-Match'] = etag
            conn.request('GET', f'{url.path}?{url.query}', headers=headers)
            response = conn.getresponse()
            # if an If-None-Match header with a previously recived etag (see above) is responded to with 304, 
            # ... then the node is already up to date in our records, report back as unchanged
            if response.status == 304:
                return False, None, None
            elif response.status == 200:
                response_headers = self.headers_to_dict(response.headers.items())
                cache_control = response_headers['Cache-Control'] if 'Cache-Control' in response_headers.keys() else None
                # TODO: control cache expiration with cahce_control
                etag = response_headers['Etag'] if 'Etag' in response_headers.keys() else None
                response_data = response.read().decode()
                g = Graph()
                g.parse(data=response_data, format='turtle')
                the_node =  self.rdf_to_node(location, g)
                the_node.etag = etag
                relations = self.rdf_get_node_relations(the_node.uri, g)
                return True, the_node, relations
            else:
                print(f"WARNING: Unexpeted HTTP response {response.status}: {response.reason}")
            
        except Exception as error:
            raise LdesClientError(f"Failed to get the node at {location}. {error}")

    def rdf_to_view(self, location: str, g: Graph) -> LdesView:
        view_description_ref = g.value(predicate=RDF.type, object=TREE.ViewDescription)
        view_alias = g.value(view_description_ref, PYLDES.alias)
        return LdesView(view_description_ref, location, view_alias)

    def rdf_to_node(self, location:str, g: Graph) -> LdesNode:
        payload = g.serialize(format='turtle')
        node_ref = g.value(None, RDF.type, TREE.Node)
        view_uri = g.value(node_ref, TREE.viewDescription)
        return LdesNode(node_ref, location, view_uri, payload, None)
    
    def rdf_get_node_relations(self, node_uri: str, g: Graph) -> List[LdesRelation]:
        result = []
        for _, __, relation_ref in g.triples((URIRef(node_uri), TREE.relation, None)):
            relation_type = g.value(relation_ref, RDF.type)
            target_node = g.value(relation_ref, TREE.node)
            the_relation = LdesRelation(node_uri, str(relation_type), str(target_node), None, False)
            result.append(the_relation)
        return result
    
    def headers_to_dict(self, headers):
        result = {}
        for key, value in headers:
            result[key] = value
        return result
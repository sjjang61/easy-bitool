import os
import yaml
import xml.etree.ElementTree as elemTree
from functools import lru_cache
from config import CONFIG_PATH

# class Settings:
#     def __init__(self, env : str = 'release' ):
#         self.settings = self.get_settings( env )
#
#     @lru_cache()
#     def get_settings(self, env):
#         with open('../resources/{env}.yml'.format(env=env)) as f:
#             return yaml.load(f, Loader=yaml.FullLoader)

# settings = Settings()
# settings.get_settings()

environment = os.getenv('APP_ENV', 'local')

@lru_cache()
def load_setting( environment=environment ):

    filename = CONFIG_PATH + '/{env}.yml'.format(env=environment.lower())
    with open(filename) as f:
        setting = yaml.load(f, Loader=yaml.FullLoader)
        return dict(setting)


class QueryManager():
    def __init__(self):
        filename = CONFIG_PATH + '/query.xml'
        self.tree = elemTree.parse(filename)

    def get_query(self, query_id ):
        query_elem = self.tree.find(f'./query[@id="{query_id}"]')
        if query_elem == None:
            print(f"[ERROR] not exist query : {query_id}")
            return None

        return query_elem.text

query_manager = QueryManager()
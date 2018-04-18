import os

from logger import *

# Pass in a dictionary config. Not JSON.
class HDriver:
    def __init__(self, config):
        self.dataset = config['dataset']
        self.output = config['output']
        self.temp_directory = config['tmp']
        self.jars = config['jars']
        self.mains = config['jar_methods']

    def _queryBuilder(self, key):
        query = """hadoop jar """ 
        query += self.jars[key]
        query += """ """
        query += self.mains[key]
        query += """ """
        query += self.dataset
        query += """ """
        query += self.output
        query += """ """
        return query

    def _dispatchAll(self):
        key = 'crime_all'
        query = self._queryBuilder(key)
        dlog("QUERY: " + query)
        os.system(query)
        return True

    def _dispatchLimit(self, l_lim, u_lim):
        key = 'crime_range'
        query = self._queryBuilder(key)
        query += l_lim 
        query += """ """
        query += u_lim
        dlog("QUERY: " + query)
        os.system(query)
        return True

    def _dispatchYear(self, year):
        key = 'crime_year'
        query = self._queryBuilder(key)
        query += str(year)
        dlog("QUERY: " + query)
        os.system(query)
        return True

    def handle(self, method, **kwargs):
        if method == 'all':
            self._dispatchAll()
        elif method == 'limit':
            self._dispatchLimit(kwargs['lower'],kwargs['upper'])
        elif method == 'year':
            self._dispatchYear(kwargs['year'])
        else:
            return False
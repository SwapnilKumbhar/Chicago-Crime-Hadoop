import os

from logger import *

class HouseKeeping:
    def __init__(self, config):
        self.tmp = config['tmp']
        self.output = config['output']
        self.filename = config['filename']

    def get(self):
        com = """hadoop fs -copyToLocal """ + self.output + self.filename + """ """ + self.tmp
        dlog("COMMAND: " + com)
        os.system(com)

    def show(self):
        com = """cat """ + self.tmp + self.filename
        os.system(com)

    def clean(self):
        rem_hadoop = """hadoop fs -rm -r """ + self.output
        rem_local = """rm """ + self.tmp + """*"""
        os.system(rem_hadoop)
        os.system(rem_local)
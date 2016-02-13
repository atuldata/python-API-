#!/opt/bin/python -u

#----------------------------
# Synopsis: schem mismatches.py --<options>
# Description: Compares the schema between two clusters
#----------------------------

import sys
import yaml
from odfi.schema import vertica_multifile_load_sql_detailed, vertica_create_sql_detailed
from erutil.rollup import run_rollups, queue_hour
from erutil.OnlineDB import OnlineDB
from erutil.EtlLogger import EtlLogger
from erutil.JobLock import JobLock
import time
from optparse import OptionParser
from datetime import datetime
import os
import subprocess
import re
import smtplib



class schema_mismatch:

   def __init__(self, yaml_file): 
      # Bootstrap environment
      self.config = yaml.load(open(yaml_file))
      self.env    = yaml.load(open(self.config['ENV']))

      self.db = OnlineDB( self.env['DSN'] )
      set_schema_sql = self.env['SET_SCHEMA_SQL']
      self.db.executeSQL(set_schema_sql, True)


   def find_mismatch(self):
      #table_mismatch = self.db.retrieveSQL()
      self.db.executeSQL(self.config['INSERT_TABLE_MISMATCH'], True)
      self.db.executeSQL(self.config['INSERT_COLUMN_MISMATCH'], True)
      #[(res,)] = self.db.retrieveSQLArgs(check_sql, args[0])

   def transfer_schema_data(self):
    first_host = self.config['FIRST_HOST']
    second_host = self.config['SECOND_HOST']   
    self.db.executeSQL(self.config['TRUNCATE_TEMP_COLUMNS'], True)
    self.db.executeSQL(self.config['TRUNCATE_TABLE_MISMATCH'], True)
    self.db.executeSQL(self.config['TRUNCATE_COLUMN_MISMATCH'], True)
     	
    query="time /opt/vertica/bin/vsql -h "+second_host+" -U "+self.env['VERTICA_USER']+" -w "+self.env['VERTICA_PASSWORD']+" -c "
    query=query+" \"CONNECT TO VERTICA DW USER "+self.env['VERTICA_USER']+" PASSWORD '"+self.env['VERTICA_PASSWORD']+"' ON '"+first_host+"',"+self.env['TCP_PORT']+";"
    query=query+"EXPORT TO VERTICA DW.schema.temp_columns AS select * from DW.v_catalog.columns;\""
    print query
    subprocess.call(query,shell=True)
             
if __name__ == "__main__":
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    g_logger = EtlLogger.get_logger(sys.argv[0].replace('.py', ''))
    g_logger.info( "Begin load: %s", datetime.now())
    g_logger.info("Starting cluster data comparator ...")
    dh = schema_mismatch(yaml_file)
    start_time = time.time()
    dh.transfer_schema_data()
    dh.find_mismatch()
    end_time = time.time()
    g_logger.info("Finished comparison. Took %s seconds" % (end_time - start_time))

    
 

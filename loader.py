#!/opt/bin/python -u

# ETL Name      : loader.py
# Purpose       : Loads demand parnter buyer ids from buyer db

import traceback
import os
import pyodbc
import sys
import datetime
import logging
import requests
import yaml
import time
from urlparse import urlparse
from urllib2 import HTTPError
import simplejson
import ox3apiclient

sys.path.append(os.path.realpath(sys.argv[0]))
config = yaml.load( open( sys.argv[0].replace(".py",".yaml")  ))

class loader:

  def __init__( self):
        #  Reading configuration file ( YAML file )
      self.logger=EtlLogger.get_logger(self.__class__.__name__)  # use class name as the log name
      self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name

      self.env= yaml.load( open( config['ENV'] ) )
      self.db = OnlineDB( self.env['DSN'], logger=self.logger )
      set_schema_sql = self.env['SET_SCHEMA_SQL']
      self.db.executeSQL( set_schema_sql )
      self.pid = os.getpid()
      self.tempCSVfile = config['brand'] + "_" + str(self.pid) + ".csv"
      self.config = config
  
  def v1_api_init(self,domain = None, realm = None, consumer_key = None, consumer_secret = None):
    V1_API_CRED_PATH=self.env['V1_API_CRED_PATH'] 
    f = open(V1_API_CRED_PATH, "r")
    creds = simplejson.loads(f.read())
    f.close()
    ox = ox3apiclient.Client(
         email = creds['email'],
         password = creds['password'],
         domain = domain or urlparse(creds['api_url']).hostname,
         realm = realm or creds['realm'],
         consumer_key = consumer_key or creds['api_key'],
         consumer_secret = consumer_secret or creds['api_secret'],
         request_token_url = "%sapi/index/initiate" % creds['sso_url'],
         access_token_url = "%sapi/index/token" % creds['sso_url'],
         authorization_url = "%slogin/process" % creds['sso_url']
    )
    ox.logon(creds['email'], creds['password'])
    return ox

  def writeDemandPartnersToTemp( self,ox):
      # Write all demand partners to a file
      start_time = time.time() 
      tmpFile = open(self.tempCSVfile , "wb")          
      res = ox.request(ox._resolve_url("brand_path"), method='GET')
      marketbrandgroupIdList=simplejson.loads(res.read())
      output={}
      try:
        for marketbrandgroupId in marketbrandgroupIdList:
            marketgroupData=ox.get("brand_path"+str(marketbrandgroupId))
            brandIdList=ox.get("brand_path_list"+str(marketbrandgroupId))
	    for brandId in brandIdList:
	      try:
	        result=ox.get("brand_path_list"+str(brandId))
                if brandId in output:
		  
        marketbrandId=ox.get("brand_name")
	Id=list(set(marketbrandId)-set(output.keys()))
	
	for brandId in Id:
	  result=ox.get("path_to_api"+str(brandId))
	  output[brandId]=[result['name'],result['TLD'],""]	
      except:
        e = sys.exc_info()[0]
        self.logger.error("Error: %s", e)
        pass
       
      finally:
        ox.logoff()
      for key, value in output.iteritems():		
        tmpFile.write((str(key) + "," +
        "\"" + value[0].replace('"', '\\"') + "\"" + 
        "\"" + value[1].replace('"', '\\"') + "\"" + 
        "\"" + value[2].replace('"', '\\"') + "\"" + 
        "\n").encode('utf-8')) 
      tmpFile.close() 
      
      end_time = time.time() 
      self.logger.info("Finished reading  into %s: %s seconds", self.tempCSVfile, (end_time - start_time))
 
  
if __name__ == "__main__":
     dpdl = loader()
     try:
       if not dpdl.lock.getLock():
          dpdl.logger.info("Unable to get lock, exiting...")
          sys.exit(0)

       dpdl.logger.info("Start running loader")
       qStart = time.time()
       
	 
       ox=dpdl.v1_api_init()
       
       dpdl.writeDemandPartnersToTemp(ox)
       
       dpdl.loadDemandPartners()

       qEnd = time.time()
       dpdl.logger.info("Finished running loader, total elapsed time: %s seconds", (qEnd-qStart))

     except SystemExit:
       pass
     except:
       e = sys.exc_info()[0]
       dpdl.logger.error("Error: %s", e)
       raise

     finally:
       dpdl.lock.releaseLock()
       try:
         os.remove(dpdl.tempCSVfile)
       except: 
         pass
       
 

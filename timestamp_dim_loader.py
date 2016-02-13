#!/opt/bin/python -u
# ETL Name      : timestamp_dim_loader.py
# Purpose       : Generate timestamp data hourly

import os
import pyodbc
import sys
from datetime import datetime, timedelta
from optparse import OptionParser
import logging
from erutil.OnlineDB import OnlineDB
from erutil.EtlLogger import EtlLogger
from erutil.JobLock import JobLock
import yaml
import time

sys.path.append(os.path.realpath(sys.argv[0]))
config = yaml.load( open( sys.argv[0].replace(".py",".yaml")  ))

class timestamp_dim_loader:
        def __init__( self ):
            #  Reading configuration file ( YAML file )
            self.logger=EtlLogger.get_logger(self.__class__.__name__)  # use class name as the log name
            self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
            self.env= yaml.load( open( config['ENV'] ) )
            self.db = OnlineDB( self.env['DSN'], logger=self.logger )
            set_schema_sql = self.env['SET_SCHEMA_SQL']
            self.db.executeSQL( set_schema_sql )
            self.config = config

        def loadTimestamp( self,date_sid):
            row_sql=config['CHECK_COUNT'] %(date_sid)
	    existing_rows = self.db.retrieveSQL(row_sql)
	    if not existing_rows:
	     date_part=date_sid[0:4]+"-"+date_sid[4:6]+"-"+date_sid[6:8]
             self.logger.info("Starting to load hourly timestamp")
	     sql=config['INSERT_NEW_RECORD'] %(date_sid,date_part,date_part)
	     self.db.executeSQL(sql, False)
             self.logger.info("inserted records for %s" %(date_part))
     	def updateLoadState(self):
      	# update load_state and commit
	   self.db.updateLoadStateWithCurrentTime(self.config['LOAD_STATE_VAR'])
      	   self.db.commit()
      	   self.logger.info("Updated load_state variable %s and  committed txn" % ( self.config['LOAD_STATE_VAR']))	           

if __name__ == "__main__":
    parser = OptionParser(usage="""usage: %prog [date_sid]  
date_sid:  date value in YYYYMMDD format to insert the timestamp details for that day (example: 20150401, will insert 24 timestamp hourly record""")
    
    (options, args) = parser.parse_args()

    if len(args)==1:
        date_sid=args[0]
    else:
        date_sid=datetime.now().strftime('%Y%m%d')


    qStart = time.time()    
    dp = timestamp_dim_loader()
     	
    try:
     if not dp.lock.getLock():
        dp.logger.info("Unable to get lock, exiting...")
        sys.exit(0)
     dp.logger.info("================================================================================================");
     dp.logger.info("Start running timestamp_dim, date_sid=%s" %(date_sid))
     dp.loadTimestamp(date_sid) 
     dp.updateLoadState()
    except SystemExit:
     pass
    except:
     e = sys.exc_info()[0]
     dp.logger.error("Error: %s", e)
     raise
    finally:
     dp.lock.releaseLock()
    qEnd = time.time()
    dp.logger.info("Finished running timestamp_dim_loader, total elapsed time: %s seconds", (qEnd-qStart))



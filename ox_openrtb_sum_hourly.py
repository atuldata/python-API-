#!/opt/bin/python -u

import sys
import yaml
from odfi.schema import vertica_multifile_load_sql_detailed, vertica_create_sql_detailed
from erutil.rollup import run_rollups, queue_hour
from erutil.OnlineDB import OnlineDB
from optparse import OptionParser
from datetime import datetime

class ox_openrtb_sum_hourly:

   def __init__(self, yaml_file, intrvl, feed_hour, data_files, schema_file): 
      # Bootstrap environment
      self.config = yaml.load(open(yaml_file))
      self.env    = yaml.load(open(self.config['ENV']))
      self.rollup_config = yaml.load(open(self.config['ROLLUP_CONFIG']))

      self.db = OnlineDB( self.env['DSN'] )
      self.intrvl = intrvl
      self.feed_hour = feed_hour
      self.data_files = data_files
      self.schema_file = schema_file
      set_schema_sql = self.env['SET_SCHEMA_SQL']
      self.db.executeSQL(set_schema_sql, True)

   def load_temporary_table(self):
      tmp_table_name = self.config['FEED_TMP_TABLE_NAME']
      drop_stmt = "DROP TABLE IF EXISTS %s" % (tmp_table_name)
      print "Removing temporary table %s" % (tmp_table_name)
      self.db.executeSQL(drop_stmt, True)
    
      feed_columns = self.config['FEED_COLUMNS'] 
      orderby = self.config['FEED_TMP_TABLE_ORDER_BY_CLAUSE']
      segmentation = self.config['FEED_TMP_TABLE_SEGMENTATION_CLAUSE']

      create_stmt = vertica_create_sql_detailed(self.schema_file,
         tmp_table_name, feed_columns, temp=True, pretty=False, orderby=orderby, segmentation=segmentation)
      print "Creating temporary table %s using column mapping: %s" % ( tmp_table_name, feed_columns) 
      self.db.executeSQL(create_stmt, commit=True)

      load_stmt = vertica_multifile_load_sql_detailed(self.data_files, self.schema_file,
         tmp_table_name, feed_columns, local=True, pretty=True)

      print "Loading table from data file %s" % ( self.data_files )
      result_count = self.db.executeSQL(load_stmt, commit=True)

      # Sanity check: the data should be for the hour ODFI claims it's for
      wrong_hour_result = self.db.retrieveSQLArgs(self.config['FEED_TMP_CHECK_HOUR'], self.feed_hour)
      if wrong_hour_result:
         wrong_hour_count = wrong_hour_result[0][0]
         if wrong_hour_count > 0:
            raise Exception("ERROR: File(s) %s contain %d rows not for the current hour (%s)" % 
              (self.data_files, wrong_hour_count, self.feed_hour))

      return result_count
   def max_element(self,itemList):
     counter = {}
     maxItemCount = 0
     mostPopularItem=itemList[0][0]    
     for item in itemList:
       if item[0][0] in counter.keys():
        counter[item[0][0]] += 1
       else:
        counter[item[0][0]] = 1
       if counter[item[0][0]] > maxItemCount:
        maxItemCount = counter[item[0][0]]
        mostPopularItem = item[0][0]
     return mostPopularItem

   def update_carrier_data(self):
      code=self.db.retrieveSQLArgs(self.config['GET_MISSING_CARRIER_CODES'],())
      code_part=[x[0].encode('UTF-8') for x in code if x[0] is not None]
      print code_part 
      for codes in code_part:         
        if codes.find(',')>0:
          single_codes=codes.split(',')
          final_code={}            
          for single_code in single_codes:
            print "Single code from collection : %s" %(single_code)
            try:
                code_name=self.db.retrieveSQL(self.config['GET_SINGLE_CARRIER']+"'"+single_code+"'")
            except:
                print 'Problem retrieving carrier by single code: %s' %(single_code)
                code_name=None
            print '%s (Single Code) -> %s (Code Name)' %(single_code, code_name)
            if code_name: 
              final_code[single_code]=code_name
          final_code_name="Unknown - Unknown"
          if final_code:
            final_code_name=self.max_element(final_code.values())
          print "Missing code_name inserting into carrier_dim",final_code_name
          self.db.executeSQLArgs(self.config['INSERT_CARRIER_DIM'], ((final_code_name,codes,datetime.now().strftime('%Y-%m-%d %H:%M:%S'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
                 
        else:
          single_code=codes
          print "Single code : %s" % (single_code)
          try:
            code_name=self.db.retrieveSQL(self.config['GET_SINGLE_CARRIER']+"'"+single_code+"'")
          except:
            print 'Problem retrieving carrier by single code: %s' %(single_code)
            code_name=None
          print '%s (Single Code) -> %s (Code Name)' %(single_code, code_name)
          if not code_name or not code_name[0][0]:
            self.db.executeSQLArgs(self.config['INSERT_CARRIER_DIM'], (("Unknown - Unknown",codes,datetime.now().strftime('%Y-%m-%d %H:%M:%S'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))    
          else:
            self.db.executeSQLArgs(self.config['INSERT_CARRIER_DIM'], ((code_name[0][0],codes,datetime.now().strftime('%Y-%m-%d %H:%M:%S'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))    
      
   def remove_existing_data(self):
      print "DELETE_EXISTING_HOUR: Removing data for hour %s " % (self.feed_hour)
      self.db.executeSQLArgs(self.config['DELETE_EXISTING_HOUR'], ((self.feed_hour,self.feed_hour)))

   def insert_merged_data(self):
      row_count = self.db.executeSQL(self.config['INSERT_DATA'], False)
      print "INSERT_DATA: Inserted %d rows for hour %s" % ( row_count, self.feed_hour )

   def updateLoadState(self):
      # update load_state and commit
      self.db.updateLoadStateWithFeedIntrvl(self.config['LOAD_STATE_VAR'], self.intrvl)
      self.db.commit()
      print "Updated and Committed load_state variable %s for %s" % ( self.config['LOAD_STATE_VAR'], self.intrvl)
   
   def queueDailyRollupJobsForRepublishing(self):
      queue_hour(self.db, self.rollup_config, 'ox_openrtb_sum_hourly', 'day', self.feed_hour)

   def run_rollups(self):
      run_rollups(self.db, self.rollup_config, 'ox_openrtb_sum_hourly', self.feed_hour)

if __name__ == "__main__":
   yaml_file = sys.argv[0].replace(".py", ".yaml")
   parser = OptionParser(usage="""usage: %prog [options] date hour schema datafile [, datafile...]
 
date:     yyyy-mm-dd    (example: 2013-06-23)
hour:     hh            (example: 20)
schema:   path          (example: /var/feeds/cache/ox_xxx/OX_XXX_1.xml)
datafile: path          (example: /var/feeds/cache/ox_xxx/OX_XXX/2031.1)""")

   parser.add_option('-r', '--republish', action="store_true", dest="republish",
      help="Process this feed as a republishing job", default=False)
   (options, args) = parser.parse_args()

   if len(args) < 4:
      parser.error("wrong number of arguments (%d)" % (len(args)))

   intrvl = args[0]+"_"+args[1]
   feed_hour = datetime.strptime("%s %s UTC" % (args[0], args[1]), '%Y-%m-%d %H %Z')
   schema_file =args[2]
   data_files = args[3:]

   dh = ox_openrtb_sum_hourly(yaml_file, intrvl, feed_hour, data_files, schema_file)
   rowcount = dh.load_temporary_table()

   dh.remove_existing_data()
   dh.update_carrier_data()
   dh.insert_merged_data()
   dh.updateLoadState()
   if options.republish:
      dh.queueDailyRollupJobsForRepublishing()
   else:
      dh.run_rollups()



# coding: utf-8

# In[ ]:

import boto
import os
import threading
from datetime import datetime
from time import sleep
import psycopg2 as pg2
REDSHIFT_PARALLEL_UNLOAD_COUNT = 4


# In[ ]:

threadlimiter = threading.BoundedSemaphore(REDSHIFT_PARALLEL_UNLOAD_COUNT)
conn_dict = {
    'host'   : "host",
    'dbname' : "dbname",
    'user'   : "user",
    'password' : "password",
    'port'   : "port"
}

s3dict = {
    
    'bucketname' : 'bucketname',
    'folder' : 'folder',
    'prefix' : 'prefix',
    's3AccessKeyID' : 's3AccessKeyID',
    's3SecretAccessKey' : 's3SecretAccessKey'
}


# In[ ]:

def connect_to_redshift(conn_dict = conn_dict):
    return pg2.connect(
        host = conn_dict['host'],
        dbname = conn_dict['dbname'], 
        user = conn_dict['user'],
        password = conn_dict['password'],
        port = conn_dict['port']
    )

def escape_sql(sql):
    return sql.replace("'","\\\'")

def generate_unload_table(schema,table,s3dict = s3dict):
    
    UNLOAD_STMT = "unload ('select * from " + schema + "." + table +"') to 's3://" +                  s3dict['bucketname'] + "/" + s3dict['folder'] + "/"+ table+"/" + s3dict['prefix'] +"' access_key_id '"+                  s3dict['s3AccessKeyID'] +"' secret_access_key '" + s3dict['s3SecretAccessKey'] +"' delimiter '|' " +                  "ADDQUOTES ESCAPE ALLOWOVERWRITE;"
    return UNLOAD_STMT

def generate_unload_sql(sql, s3dict = s3dict):
    UNLOAD_STMT = "unload ('" + escape_sql(sql) +"') to 's3://" +                  s3dict['bucketname'] + "/" + s3dict['folder'] + "/" + s3dict['prefix'] +"' access_key_id '"+                  s3dict['s3AccessKeyID'] +"' secret_access_key '" + s3dict['s3SecretAccessKey'] +"' delimiter '|' " +                  "ADDQUOTES ESCAPE ALLOWOVERWRITE;"                     
    return UNLOAD_STMT


# In[ ]:

class unloadTable(threading.Thread):
    def __init__(self, schema=None, table=None, sql=None, connection=None, s3dict=s3dict):
        
        threading.Thread.__init__(self)
        self.tablename = table
        self.schemaname = schema
        self.conn = connection
        self.sql = sql
        self.result = None
        if self.sql != None:
            self.unload_cmd = generate_unload_sql(self.sql,s3dict)
        elif self.tablename != None:
            self.unload_cmd = generate_unload_table(self.schemaname, self.tablename, s3dict)
        else:
            self.result = -1
            raise Exception("Table or sql should be provided to unload")
        
    def run(self):
        threadlimiter.acquire()
        self.exc = None
        with self.conn.cursor() as curr:
            try:
                print "starting " + self.tablename
                curr.execute("set statement_timeout = 0")
                curr.execute(self.unload_cmd)
            except Exception as e:    
                self.exc = e
            finally:
                print self.tablename + " exiting"
                threadlimiter.release()   

    def join(self):
        super(unloadTable,self).join()
        print self.schemaname + "." + self.tablename +" joining master"
        if self.exc:
            raise self.exc


# In[ ]:

def unload_these_tables(tablelist=None,filelist=None,s3dict=s3dict,conn_dict=conn_dict):
    connection = connect_to_redshift(conn_dict)
    with connection.cursor() as cur:
        cur.execute("select current_timestamp")
        stime = cur.fetchall()[0][0].strftime('%Y-%m-%d %H:%M:%S')
    threadlist=[]
    if tablelist != None:
        for table in tablelist:
            schema=table.split(".")[0]
            tablename = table.split(".")[1]
            sql = None
            t=unloadTable(schema,tablename,sql,connection,s3dict)
            threadlist.append(t)
            t.start()
    elif filelist!=None:
        for filename in filelist:
            with open(filename,"r") as sqlfile:
                sql = sqlfile.read()
            schema = None
            tablename = None
            t = unloadTable(sql=sql,connection = connection,s3dict=s3dict)
            threadlist.append(t)
            t.start()
    else:
        raise Exception("Please provide either a list of tables or a list of sql files")
    for t in threadlist:
        t.join()
    connection.reset()
    sleep(180)
    with connection.cursor() as cur:
        cur.execute("select current_timestamp")
        etime = cur.fetchall()[0][0].strftime('%Y-%m-%d %H:%M:%S')
        AUDITSQL="select query,split_part(split_part(path,'/',5),'_0',1) as tablename,sum(line_count) " +                 "as rowcount from stl_unload_log where start_time >='" + stime +"' and end_time<= '" +                  etime + "' group by 1,2 order by 2;"
        print AUDITSQL
        cur.execute(AUDITSQL)
        auditresults = cur.fetchall()
    return auditresults


# In[ ]:

def main():
    s3creds = {
        'bucketname' : 'xxxxxxx',
        'folder' : 'xxxxxx',
        'prefix' : 'data_',
        's3AccessKeyID' : 'xxxxxxxxxxxxxxxxx',
        's3SecretAccessKey' : 'xxxxxxxxxxxxxxxxxxxxxxxx'
    }
    redshiftcreds = {
        'host'   : "xxxxxxxxxxxxxxxxxxxxxxx",
        'dbname' : "xxxxxxxxxxxx",
        'user'   : "xxxxxxxxxxxx",
        'password' : "xxxxxxxxxxxxxxxxxx",
        'port'   : "5439"
    }
    
    tablist = [
    'my_schema.my_table1',
    'my_schema.my_table2',
    'my_schema.my_table3',
    'my_schema.my_table4',
    'my_schema.my_table5'
    ]
    
    print unload_these_tables(tablist,None,s3creds,redshiftcreds)


# In[ ]:

if __name__ == '__main__':
    main()
    


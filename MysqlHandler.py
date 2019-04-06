import mysql.connector
from enum import Enum

"""
@Autor : Liran Nachman

MydbHandler is class that help you to created and executed easily queries 

Methods :

- init: init configure on database details and authorization 

- selectHelper: create a select query 

- execute: Fetch query from the database, Noted They are two ways to fetch - one row  or all rows selected

- close : close connection your database ***important
 """


class FetchType(Enum):
    FETCH_ALL = "all",
    FETCH_ONE = "one"

class MydbHandler:

    """
    init method
    parms:localhost  - host\domain name 
    parms : username - your username of database
    parms : password - your password of database
    parms : databasename - your name database
    """
    def __init__(self,localhost:"host web name",username : "Your username to database",
        password : "your password to database",databasename:"your name database "):
        self.mydb = mysql.connector.connect(
        host=localhost,
        user=username,
        passwd=password,
        database=databasename)

 
    
    """
    SelectHelper method
    parms:tablename  - name of table 
    parms : fields - a list of fields that your want to get
    parms : filters - list of tuples like [("age","13"),("name","liran")] must be strings
    parms : databasename - your name database
    """
    
    def SelectHelper(self,tablename:"Name of table that i want selected"=None
        ,fields:"list of fields"=[]
        ,filters:"list of tuples (key,value)"=[])->'string query':

        if tablename is None:
            raise BaseException("Must input table name")

        # create WHERE SENTANCES KEY=VALUE
        whereas = list(map(lambda item :item[0]+"='"+item[1]+"'",filters)) 

        # add fields or all if empty
        query = "SELECT *" if len(fields) == 0 else "SELECT "+str(fields)[1:-1].replace("'","")

        query += " FROM "+tablename

        ## add filter in end of query
        if len(whereas) > 0:
                query+=" WHERE "+whereas[0]         

        if(len(whereas) > 1):
            for w in whereas[1:]:
                query+=" AND "+w 


        return query


    """
    execute method
    parms : query - a string that created by SelectHelper or any query
    parms : fetchtype - FetchType.FETCH_ONE or FetchType.FETCH_ALL of FetchType class
    """
    def execute(self,query:"Query string"
        ,fetchtype:FetchType) -> 'list of tuples of results selected':

        if not(fetchtype is FetchType.FETCH_ALL or fetchtype is FetchType.FETCH_ONE):
            raise Exception("Unvalid FetchType,must be only FetchType.FETCH_ALL or FetchType.FETCH_ONE")

        mycursor = self.mydb.cursor()
        mycursor.execute(query)
        myresult = mycursor.fetchall() ## fetch all
        if len(myresult) > 0:
            if fetchtype is FetchType.FETCH_ONE: # fetch one 
                myresult = myresult[0]
        else:
            return [] ## empty results
    
        return myresult


    """
    close Method
    close connection from database 
    """
    def close(self):
        self.mydb.close()

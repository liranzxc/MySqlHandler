from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.engine.url import URL
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import column
from enum import Enum
from setting import Setting


class FetchType(Enum):
    FETCH_ALL = "all",
    FETCH_ONE = "one"


class ConstDic(Enum):
    TABLENAME = "TableName",
    SELECTED = "selected",
    FILTER = "filter"
    TYPEFETCH = "typeFetch"


class MySqlHandlerAcademy:
   
    def __init__(self,localhost:"host web name",username : "Your username to database",
        password : "your password to database",databasename:"your name database "):
        """
        param : localhost -string  host\domain name \n 
        param : username -string your username of database \n
        param : password -string your password of database \n
        param : databasename -string your name database \n """
        


        url_mysql = {'drivername': 'mysql',
               'username': username,
               'password': password,
               'host': localhost,
               'port': 3306
               ,'database':databasename}

        ## create URL 
        url_mysql = (URL(**url_mysql))
        self.engine = create_engine(url_mysql,echo=False)
        self.conn = self.engine.connect()
        self.meta = MetaData(self.engine,reflect=True)
        ## connection
        """finish connection  """


    def Select(self,MyQueryDic:dict) -> "List of tuples results or None if empty":
        """parms : dict like example : \n
        MyQueryDic = {   \n
        ConstDic.TABLENAME : "Users", \n
        ConstDic.SELECTED : ["UserID"], \n
        ConstDic.FILTER : {            \n
                    "UserID" :1234567 \n                         
        }, \n
       ConstDic.TYPEFETCH : FetchType.FETCH_ONE \n
        }
        
        """
                
        if MyQueryDic.get(ConstDic.TABLENAME,None) is not None:
            table = self.meta.tables[MyQueryDic[ConstDic.TABLENAME]]
            SelectedColumns =[]
            if MyQueryDic.get(ConstDic.SELECTED,None) is not None and len(MyQueryDic.get(ConstDic.SELECTED)) > 0:
                for key in MyQueryDic.get(ConstDic.SELECTED):
                    SelectedColumns.append(column(key))
            else:
                SelectedColumns.append(table)

            Wheres = []
            if(MyQueryDic.get(ConstDic.FILTER,None) is not None):
                for keyFilter,ValueFilter in MyQueryDic.get(ConstDic.FILTER).items():
                    Wheres.append(column(keyFilter) == ValueFilter)

           
            stmt = select(SelectedColumns).where(and_(*Wheres)).select_from(table)
           
            if MyQueryDic.get(ConstDic.TYPEFETCH,FetchType.FETCH_ALL) == FetchType.FETCH_ALL:
                results = self.conn.execute(stmt).fetchall()
            else:
                results = self.conn.execute(stmt).fetchone()

            ## print Query
            #print(stmt)
            return results


    def Insert(self,TableName,_UserID,_Points,_Date):
        table = self.meta.tables[TableName]
        self.conn.execute(table.insert(),UserID=_UserID,Points=_Points,Date=_Date)


    def close(self):
        """
        close connection from database
        """
        self.conn.close()

    
if __name__ == "__main__":
    _setting = Setting()
    db = MySqlHandlerAcademy(_setting.setting["host"],_setting.setting["username"],_setting.setting["password"],_setting.setting["database"])
    #db.Insert("TABLE 3",[])
    db.close()
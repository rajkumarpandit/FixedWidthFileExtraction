import cx_Oracle
import conf.rkUtils as util

##os.putenv('ORACLE_HOME', 'C:\App\rajpandi\product\11.2.0\client_1\bin')
##os.putenv('LD_LIBRARY_PATH', 'C:\App\rajpandi\product\11.2.0\client_1\lib')

###following worked fine
#def getConnection():
#    con = cx_Oracle.connect("stars", "TreaS2018", "FNTR2DEV")    
#    #con.close()
#    return(con)
###end- of worked code


class Oracle(object):

    schema = util.getKeyVal("DB_USER")
    sid = util.getKeyVal('DB_SID')        
    
    def getConnection(self, p_env):
        """
        use this when caller needs to iterate cursor
        """
        try:
            #self.db = cx_Oracle.connect("stars", "TreaS2018", "FNTR2DEV")   
            if  p_env =='STG':
                self.sid = util.getKeyVal('DB_SID_STG')
            elif p_env =='PRD':
                self.sid = util.getKeyVal('DB_SID_PRD')
            else:
                self.sid = util.getKeyVal('DB_SID_DEV')
            self.pwd = util.getDbPwd(p_env)   
            self.db = cx_Oracle.connect(self.schema, self.pwd, self.sid)
            return(self.db)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            raise
        
    def connect(self):
        """ 
        Connect to the database.
        User this when the caller doesn't need the cursor 
        
        """

        try:
            self.db = cx_Oracle.connect(self.schema, self.pwd, self.sid) 
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            raise

        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.cursor = self.db.cursor()
        
    def closeConnection(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        to be used in connection with getConnection()
        """

        try:

            self.db.close()
        except cx_Oracle.DatabaseError:
            pass
    def disconnect(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        to be used in connection with connect()
        """

        try:
            self.cursor.close()
            self.db.close()
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=None, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        use this when nothing in return needed
        """

        try:
            self.cursor.execute(sql, bindvars)            
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            raise

        # Only commit if it-s necessary.
        if commit:
            self.db.commit()
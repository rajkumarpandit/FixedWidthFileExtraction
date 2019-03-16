import connectionFactory as cf 
import pandas as pd
from itertools import groupby
import string
from random import choice
import random
import fnmatch
import os
import glob
import re
import arrow
import cx_Oracle 
import conf.rkUtils as util
import shutil
import json 
import datetime
import xlrd 

staging_path = util.getKeyVal('STAGING_PATH')
temp_file_pattern = '*.rk'

SUCCESS = 'Success'
FAILED = 'Failed'
INPROCESS = 'In-process'


#---------------------
#generate random string
#------------------------

def genRandomString(length=8, chars=string.ascii_letters + string.digits):
    return ''.join([choice(chars) for i in range(length)])

#----------------------------------------
#count number of members in a nested list
#----------------------------------------

def count(lst):
    return sum(1+count(i) for i in lst if isinstance(i,list))

#----------------------------------------
#check if a given date is i right format
#----------------------------------------
#fmts = ('%Y','%b %d, %Y','%b %d, %Y','%B %d, %Y','%B %d %Y','%m/%d/%Y','%m/%d/%y','%b %Y','%B%Y','%b %d,%Y')
#"Jan 19, 1990        " => "%b %d, %Y           " => 1990-01-19 00:00:00
#"January 19, 1990    " => "%B %d, %Y           " => 1990-01-19 00:00:00
#"Jan 19,1990         " => "%b %d,%Y            " => 1990-01-19 00:00:00
#"01/19/1990          " => "%m/%d/%Y            " => 1990-01-19 00:00:00
#"01/19/90            " => "%m/%d/%y            " => 1990-01-19 00:00:00
#"1990                " => "%Y                  " => 1990-01-01 00:00:00
#"Jan 1990            " => "%b %Y               " => 1990-01-01 00:00:00
#"January1990         " => "%B%Y                " => 1990-01-01 00:00:00
def check_date_format(p_date_value, p_date_format):
    try:
        if p_date_format=='MM/DD/YYYY':
            dt = datetime.datetime.strptime(p_date_value, "%m/%d/%Y")
        elif p_date_format=='YYYY-MM-DD':
            dt = datetime.datetime.strptime(p_date_value, "%Y-%m-%d")
        elif p_date_format=='YYYYMMDD':
            dt = datetime.datetime.strptime(p_date_value, "%Y%m%d")            
        
    except:
        return FAILED
    return SUCCESS

#-------------------------------------------------------
#utility function for HTML based email content creation
#------------------------------------------------------

def add_tr(p_in_str):
    return "<tr>"+p_in_str+"</tr>"

def add_td(p_in_str, p_row_span=0, p_col_span=0):
    
    if p_col_span!=0 and p_row_span ==0:
        ret_str = "<td colspan='"+str(p_col_span)+"'>"+p_in_str+"</td>"
    elif p_row_span!=0 and p_col_span==0:
        ret_str = "<td rowspan='"+str(p_row_span)+"'>"+p_in_str+"</td>"
    else:
        ret_str = "<td>"+p_in_str+"</td>"   

    return ret_str

#-----------------------
#make tuples from cursor
#-----------------------

def rows_to_dict_list(cur):
    columns = [i[0] for i in cur.description]
    return [dict(zip(columns, r)) for r in cur]

#---------------------------
#Getting basic vendor data
#---------------------------

def get_vendor_data():
    cur = con.cursor()
    try:
        cur.execute("select CONF_ID,VENDOR_NAME,ROW_IDENTIFIER_LENGTH,WILD_CARD,INBOUND_PATH,ARCHIVE_PATH,FAILED_PATH,FILE_CODING, FILE_TYPE, DATE_FORMAT from RK_FWF_CONF_MAST WHERE FLG_ENABLE ='Y' order by 1")
        ret_dict =  rows_to_dict_list(cur);
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in get_vendor_data***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED          
    return ret_dict

#-------------------------------------------------------------------------------------
#Getting allowed list of record types
#only already registered record types will be taken into consideration for processing
#Anything that comes into file will not be processed
#--------------------------------------------------------------------------------------

def get_record_types(p_conf_id):    
    cur = con.cursor()
    rts=[]
    try:
        cur.execute("select to_char(RECORD_TYPE) RECORD_TYPE from RK_FWF_CONF_RECORD_TYPE_MAST where conf_id = {}".format(p_conf_id) )
        rts = [row[0] for row in cur.fetchall()]
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in get_record_types***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED          
    return rts


#------------
#get load ID
#------------

def genLoadID():         
    cursor = con.cursor()
    lid=0
    try:
        cursor.execute("select rk_seq.nextval  from dual")
        lid= cursor.fetchone()[0]
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in genLoadID***')
        error, = ex.args    
        print ('Error.message ='+ error.message)
        return -1
    cursor.close()  
    return lid

#-----------------------------
#insert file details to track
#-----------------------------

def add_load_details(p_file_name, p_vendor_name, p_status):
    cur = con.cursor()
    
    if p_status==INPROCESS:
        qry = "INSERT INTO RK_LOAD_DETAILS(LOAD_ID,FILE_NAME,VENDOR_NAME,STATUS,CREATED_DATE,CREATED_BY) Values ("+str(load_id)+", '"+p_file_name+"', '"+p_vendor_name+"', '"+p_status+"', SYSDATE, 'RK_JOB' ) "
    else:
        qry = "Update RK_LOAD_DETAILS set STATUS='"+p_status+"' where load_id = "+str(load_id)
    
    try:
        cur.execute(qry)
        processed_row_count = cur.rowcount
        if processed_row_count<=0:
            print("***Load details not updated***")
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in add_load_details***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED        
    return SUCCESS
    
#---------------------------------------------
#Dynamically generating column specification
#---------------------------------------------

def generate_column_specification(p_conf_id, p_record_type):
    cur = con.cursor()
    start_pos=[]
    end_pos=[]
    ret_list = []
    try:
        cur.execute("select START_POSITION,END_POSITION  from  RK_FWF_CONF_COLUMN_MAST  where record_type = '"+p_record_type+"' and conf_id = {}  order by seq_no".format(p_conf_id) )
        start_pos, end_pos = zip(*cur.fetchall())
        #recs = cur.fetchall()
        #start_pos = [rec[0] for rec in recs]
        #end_pos = [rec[1] for rec in recs]
        ret_list = list(zip(start_pos,end_pos))
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in generate_column_specification***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return []          
    return ret_list

#-----------------------------------------
#Dynamically generating column headers
#-----------------------------------------

def generate_column_names(p_conf_id, p_record_type):
    cur = con.cursor()
    try:
        cur.execute("select trim(COLUMN_NAME) COLUMN_NAME  from  RK_FWF_CONF_COLUMN_MAST  where record_type = '"+p_record_type+"' and conf_id = {}  order by seq_no".format(p_conf_id) )
        col_names = [row[0] for row in cur.fetchall()]
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in generate_column_names***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return []           
    return col_names

#------------------------------------------------------------------
#Creating a list for the data type of each column of the FWF fetch
#------------------------------------------------------------------

def get_column_data_types(p_conf_id, p_record_type):
    cur = con.cursor()
    try:
        cur.execute("select DATA_TYPE  from  RK_FWF_CONF_COLUMN_MAST  where record_type = '"+p_record_type+"' and conf_id = {}  order by seq_no".format(p_conf_id) )
        col_data_types = [row[0] for row in cur.fetchall()]
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in get_column_data_types***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return []           
    return col_data_types
    


#-----------------------------------------------------------------------------------
#Recovering Values of IBM Signed Fields After EBCDIC to ASCII Character Conversion
#-----------------------------------------------------------------------------------

def get_ascii_equivalent(p_incoming_value):   

    try:
        lsd = len(p_incoming_value) - 1; # Least significant digit  
     
        
        if p_incoming_value[lsd] in ebcdic_postive:
            ret_decimal_value = p_incoming_value.replace(p_incoming_value[lsd], ebcdic_postive[p_incoming_value[lsd]] )
        
        if p_incoming_value[lsd] in ebcdic_negitve:
            ret_decimal_value = 0
            ret_decimal_value = p_incoming_value.replace(p_incoming_value[lsd], ebcdic_negitve[p_incoming_value[lsd]] )   
            ret_decimal_value = int(ret_decimal_value)*(-1) 
    except Exception as e:
        print("Error in get_ascii_equi:"+str(p_incoming_value))  
        print("type error: " + str(e))
        print("Error {}".format(e))
        ret_decimal_value='0' 
        raise ValueError('Error: get_ascii_equivalent for {}.', p_incoming_value)
    
    return ret_decimal_value; 

#---------------------------------------------------
#Generating list of bulk insert for values clause
#non jSON way
#---------------------------------------------------

def add_to_list (p_filename, p_conf_id, p_record_type): 
    col_counter =0
    column_specification=generate_column_specification(p_conf_id, p_record_type)
    column_names=generate_column_names(p_conf_id, p_record_type)  
    column_data_types=get_column_data_types(p_conf_id, p_record_type)
 
    #print(column_specification)
    #print(column_names)    
    lines_row =[] 

    df = pd.read_fwf(p_filename, colspecs=column_specification, names=column_names, header=None, converters={0:str, 1:str})

    for i, row in df.iterrows():
        #add to the nested list, the previous list present in the row
        if i>0:
            lines.append(lines_row) 
        
        #this is additional to already specified column names in the database conf data
        lines_row =[]    
        lines_row.append(load_id)
        #print("\nRow:\n"+str(row))
        col_counter = 0        
        for j, column_value in row.iteritems():
            #following is to check nan, oracle doesn't automatically convert nan to NULL. Blank values are read NAN by panda python                                             
            if column_value==column_value:                   
                if ( vendors['FILE_CODING'] == "EBCDIC" and ( column_data_types[col_counter] =='INTEGER' or column_data_types[col_counter] =='DECIMAL') ):
                    #change the EBCDIC Chars
                    lines_row.append(str(float(get_ascii_equivalent(column_value))))
                else:
                    
                    if (column_data_types[col_counter] =='DATE'):
                        if str(column_value).find('-')>0:
                            column_value=re.sub('\-', '', column_value)
                        lines_row.append(str(int(column_value)))
                    else:
                        lines_row.append(column_value)
            else:
                lines_row.append(None)                
            col_counter = col_counter +1
    lines.append(lines_row)    
    #print(lines)
    os.remove(p_filename)    
    return SUCCESS



#--------------------------------------------------
#to be used with NON JSON based input data
#to get insert statement. 
#To be used for less number of rows, less than 100
#--------------------------------------------------

def get_insert_query (p_conf_id, p_record_type): 
    cur = con.cursor()
    try:
        cur.execute("select TABLE_NAME from RK_FWF_CONF_RECORD_TYPE_MAST where RECORD_TYPE ='"+ p_record_type+ "' and conf_id = {}".format(p_conf_id) )
        result=cur.fetchone()
        table_name = result[0]
        print("Table name: "+table_name, end="")
        cur.execute("select COLUMN_NAME, to_char((SEQ_NO +1) ) SEQ_NO1   from  RK_FWF_CONF_COLUMN_MAST  where record_type = '"+p_record_type+"' and conf_id = {}  order by seq_no".format(p_conf_id) )
        col_names, col_positions =  zip(*cur.fetchall()) #[row[0] for row in cur.fetchall()] 
        #col_positions = [row[0] for row in cur.fetchall()]  
        qry= "INSERT INTO "+table_name + " (LOAD_ID, " + ",".join(col_names ) +") VALUES (:1,:" + ",:".join(col_positions ) +")" 
        #print("\tQuery :"+qry)
        cur.close();
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in get_insert_query***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return ""          
    return qry

#--------------------------------------------
#to be used with NON JSON based input data
#to insert data in the array into database
#--------------------------------------------

def insert_DB (p_filename, p_conf_id, p_record_type): 
      
    cursor = con.cursor()
    cursor.arraysize = 500
 
    query = get_insert_query(p_conf_id, p_record_type) 
    #print("\nquery:\t"+query)
    if lines:
        no_of_rows = count(lines)
        values_clause=lines    
    
    try:
        
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'RRRRMMDD'")
        cursor.executemany(query, values_clause, batcherrors = True, arraydmlrowcounts = True)
        inserted_row_count = cursor.rowcount     
        print(", Rows in the list :{}". format(no_of_rows)+", inserted :{}    ". format(inserted_row_count), end="" )
        
        if inserted_row_count !=no_of_rows:
            print("Status:Error")
            errors = cursor.getbatcherrors()
            print("Count:    ", len(errors))
            for error in errors:
                print("Error:    ", error.message.rstrip(), "    ,at row offset", error.offset)
            return FAILED
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in insert_DB***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED
    cursor.close()      
    return SUCCESS

#--------------------------------------------------
#to create JSON of the input file.
#One JSOn per record type 
#remember, file is broken into number of files
#equal to number of record types present
#--------------------------------------------------

def create_json_from_fwf (p_filename, p_conf_id, p_record_type): 
    col_counter =0
    column_specification=generate_column_specification(p_conf_id, p_record_type)
    column_names=generate_column_names(p_conf_id, p_record_type)  
    column_data_types=get_column_data_types(p_conf_id, p_record_type)
     
    global row_counter
    row_counter=0  
    row_data={}  
    file_data_json_dict = {}
    df = pd.read_fwf(p_filename, colspecs=column_specification, names=column_names, header=None, converters={dt:str for dt in column_names})
    #print(column_specification)
    #print(column_names)
    for i, row in df.iterrows():
        row_counter = row_counter + 1
        #add to the nested list, the previous list present in the row
        if i>0:
            lines.append(row_data) 
        
        #this is additional to already specified column names in the database conf data
        row_data={}     
        row_data['load_id']=load_id
        row_data['created_date']=arrow.now().format('MM/DD/YYYY HH:mm')

        col_counter = 0   
        column_value = ""     
        for j, column_value in row.iteritems():
            #following is to check nan, oracle doesn't automatically convert nan to NULL. Blank values are read NAN by panda python                                             
            if column_value==column_value:                   
                if ( vendors['FILE_CODING'] == "EBCDIC" and ( column_data_types[col_counter] =='INTEGER' or column_data_types[col_counter] =='DECIMAL') ):
                    #change the EBCDIC Chars
                    row_data[column_names[col_counter].lower()] = float(get_ascii_equivalent(column_value))
                else:
                    
                    if (column_data_types[col_counter] =='DATE'):
                        if vendors['DATE_FORMAT']=='YYYY-MM-DD' or vendors['DATE_FORMAT']=='YYYYMMDD':
                            if str(column_value).find('-')>0:
                                column_value=re.sub('\-', '', column_value)
                            if str(column_value).find('/')>0:  
                                column_value=re.sub('\/', '', str(column_value))
                            if column_value==0 or str(column_value) == '00000000' or column_value=='':   
                                row_data[column_names[col_counter].lower()] = ''                                
                            else:
                                row_data[column_names[col_counter].lower()] = str(column_value)[4:6]+"/"+str(column_value)[6:]+"/"+str(column_value)[0:4]  
                        else:
                            print('Currently this date format handing is not available')
                            return FAILED                        
                    elif column_data_types[col_counter] == 'DECIMAL' or column_data_types[col_counter] == 'INTEGER'  :
                        row_data[column_names[col_counter].lower()] = column_value.replace(',', '').replace('$', '') 
                    else:
                        row_data[column_names[col_counter].lower()] = column_value
            else:
                row_data[column_names[col_counter].lower()] = ''                
            col_counter = col_counter +1
    lines.append(row_data)    
    file_data_json_dict["data"] = lines
    global file_json_data
    #file_json_data = str(file_data_json_dict)
    file_json_data = json.dumps(file_data_json_dict)
    #print(", Json created2:"+file_json_data, end="")
    os.remove(p_filename)    
    return SUCCESS
#---------------------------------------------------------
#to create JSON of the input file that come in CSV format.
#---------------------------------------------------------

def create_json_from_csv (p_filename, p_conf_id, p_record_type): 
    col_counter =0
    #column_specification=generate_column_specification(p_conf_id, p_record_type)
    column_names=generate_column_names(p_conf_id, p_record_type)  
    column_data_types=get_column_data_types(p_conf_id, p_record_type)
    #print(column_names)
    #print(column_data_types)
    global row_counter
    row_counter=0  
    row_data={}  
    file_data_json_dict = {}
    flg_end_reached='N'
    
    #vendors["ROW_IDENTIFIER_LENGTH"] keep track of the row number(csv row number starts at 0) where the header would be for hertz it is 0 for enterprise it is 1
    df = pd.read_csv(p_filename, names=column_names, na_values=[''],  header = vendors["ROW_IDENTIFIER_LENGTH"],  converters={dt:str for dt in column_names})

    #print(column_specification)
    #print(column_names)
    for i, row in df.iterrows():

        #add to the nested list, the previous list present in the row
        if i>0:
            lines.append(row_data) 
        
        #this is to not read the summation columns, if any
  
        if len(str(df.iloc[i,0]))>0 :
            row_counter = row_counter + 1
            row_data={}     
            row_data['load_id']=load_id
            row_data['created_date']=arrow.now().format('MM/DD/YYYY HH:mm')
            row_data['seq_no']=str(load_id)+"-"+str(row_counter)
            col_counter = 0   
            column_value = ""             
            
            for j, column_value in row.iteritems():
                #following is to check nan, oracle doesn't automatically convert nan to NULL. Blank values are read NAN by panda python 
                column_value=column_value.rstrip(' ')
                column_value=column_value.lstrip(' ')
                
                if column_value==column_value: 
                    
                    if (column_data_types[col_counter] =='DATE'):
                        
                        if vendors['DATE_FORMAT']=='YYYYMMDD' or vendors['DATE_FORMAT']=='YYYY-MM-DD':
                            #below id not fully done
                            if str(column_value).find('-')>0:
                                column_value=re.sub('\-', '', column_value)
                            if str(column_value).find('/')>0:  
                                column_value=re.sub('\/', '', str(column_value))
                            if column_value==0 or str(column_value) == '00000000' :   
                                row_data[column_names[col_counter].lower()] = ''
                            else:
                                #print(column_value+" "+str(column_value)[0:2]+" " + str(column_value)[2:4]+" "+ str(column_value)[4:8])
                                row_data[column_names[col_counter].lower()] = str(column_value)[4:6]+"/"+str(column_value)[6:]+"/"+str(column_value)[0:4]   
                                                        
                        elif vendors['DATE_FORMAT'] == 'MM/DD/YYYY':
                            if column_value.find('0001-01-01') <0 or column_value == '':
                                if check_date_format(column_value, vendors['DATE_FORMAT']) == SUCCESS:
                                    row_data[column_names[col_counter].lower()] = column_value
                                else:
                                    print("\t[Row/Column]:[" +str(i)+"/"+ str(j) + "]-Date format not correct", end="")
                                    return FAILED
                            else:
                                row_data[column_names[col_counter].lower()] = ''
                        else:
                            print("\t[Row/Column]:[" +str(i)+"/"+ str(j) + "]-ATTENTION!!! This date format is yet to be handled !!!", end="")
                            
    
                    elif column_data_types[col_counter] == 'DECIMAL' or column_data_types[col_counter] == 'INTEGER'  :
                        row_data[column_names[col_counter].lower()] = column_value.replace(',', '').replace('$', '') 
                    else:
                        row_data[column_names[col_counter].lower()] = column_value 

                else:
                    row_data[column_names[col_counter].lower()] = ''                
                col_counter = col_counter +1
        else:
            print('\t.....', end="")
            #is would be useful when summation rows are added and we needed to avid them
            flg_end_reached="Y"
            
    if flg_end_reached=='N':
        lines.append(row_data)  
      
    file_data_json_dict["data"] = lines
    global file_json_data
    file_json_data = json.dumps(file_data_json_dict)
    #print(", JSON from CSV created:"+file_json_data, end="")
    #os.remove(p_filename)    
    return SUCCESS


#---------------------------------------------------------
#to create JSON of the input file that come in Excel format.
#---------------------------------------------------------

def create_json_from_excel (p_filename, p_conf_id, p_record_type, p_sheet_counter): 
    
    wb = xlrd.open_workbook(p_filename)
    sheet = wb.sheet_by_index(p_sheet_counter)
    no_of_excel_rows = sheet.nrows
    no_of_excel_cols = sheet.ncols
    column_names = generate_column_names(p_conf_id, p_record_type)  
    column_data_types = get_column_data_types(p_conf_id, p_record_type)    
    #print( column_names)
    global row_counter
    row_counter=0  
    row_data={}  
    file_data_json_dict = {}
    
    #reading excel values
    for i in range(no_of_excel_rows):
        #vendors['ROW_IDENTIFIER_LENGTH'] this tells us if the row is a header. We need not read header.
        if i == vendors['ROW_IDENTIFIER_LENGTH']:
            continue
        else:
            column_value=""
            row_data={} 
            #read the first column fo the row
            column_value = sheet.cell_value(i,0)
            #column_value=column_value.rstrip(' ')
            #column_value=column_value.lstrip(' ') 
            
            row_data['load_id']=load_id
            row_data['created_date']=arrow.now().format('MM/DD/YYYY HH:mm')
            row_data['seq_no']=str(load_id)+"-"+str(i)            
            
            #this check ensure that the last row is not read which may contain the summary of the 
            if  column_value == '' or column_value == "" or column_value==" ": 
                continue
            else:  
                if i>0:
                    row_counter = row_counter + 1
                    lines.append(row_data) 
                for j in range(no_of_excel_cols):
                    
                    column_value = sheet.cell_value(i, j)
                    
                    if column_value == column_value and not ( column_value == '' or column_value == "" or column_value==" ") : 
                        if (column_data_types[j] == 'DATE'):
                            #column_value1 = datetime.datetime(*xlrd.xldate_as_tuple(column_value, wb.datemode))
                            #print( "date:"+str( column_value))
                            dateTuple = xlrd.xldate_as_tuple(column_value, wb.datemode)
                            column_value = str( dateTuple[1] )+"/"+ str( dateTuple[2] )+"/" +str( dateTuple[0] )                             
                            #print( "date2:"+str( column_value))                              
                            if vendors['DATE_FORMAT'] == 'MM/DD/YYYY':
                                if column_value.find('0001-01-01') <0 or column_value == '':
                                    if check_date_format(column_value, vendors['DATE_FORMAT']) == SUCCESS:
                                        row_data[column_names[j].lower()] = column_value
                                    else:
                                        print("\t[Row/Column]:[" +str(i)+"/"+ str(j) + "]-Date format not correct", end="")
                                        return FAILED
                                else:
                                    row_data[column_names[j].lower()] = ''   
                        elif column_data_types[j] == 'DECIMAL' or column_data_types[j] == 'INTEGER' :
                            row_data[column_names[j].lower()] = str( column_value) #.replace(',', '').replace('$', '') 
                        else:
                            row_data[column_names[j].lower()] = column_value 
                         
                        #print(str( i)+"."+column_names[j]+":"+ str( column_value ) +","+column_data_types[j])
                    else:
                        row_data[column_names[j].lower()] = '' 
      
    file_data_json_dict["data"] = lines
    global file_json_data
    file_json_data = json.dumps(file_data_json_dict)
    #print("\t JSON from excel created:"+file_json_data )
    
    return SUCCESS
#--------------------------------------------------
#to be used when partial commit is done
#assuming after partial commit rest of the file failed 
#then the whole file needs to be rolled back
#--------------------------------------------------

def rollback_partially_committed_file (p_conf_id, p_load_id): 
    cur = con.cursor()
    try:
        qry="""
        DECLARE
            l_load_id NUMBER;
        BEGIN
            l_load_id:= :LOAD_ID;
            FOR rec in ( select TABLE_NAME from RK_FWF_CONF_RECORD_TYPE_MAST where conf_id = :CONF_ID )
            LOOP
                execute immediate 'delete from '||rec.TABLE_NAME||' where load_id= '|| l_load_id;
            END LOOP;
        END;
        
        """
        cur.execute(qry, LOAD_ID=p_load_id, CONF_ID=p_conf_id )
        cur.close()
        print("\tFailed file rolled back -> Load ID: "+str( p_load_id)+ " with Vendor id: "+str(p_conf_id))

        
    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in rollback_partially_committed_file***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED          
    return SUCCESS

#--------------------------------------------------
#to insert JSON in the data base
#--------------------------------------------------

def insert_json_to_DB (p_filename, p_conf_id, p_record_type): 
      
    cursor = con.cursor()
    
    try:
        cursor.execute("select TABLE_NAME from RK_FWF_CONF_RECORD_TYPE_MAST where RECORD_TYPE ='"+ p_record_type+ "' and conf_id = {}".format(p_conf_id) )
        table_name = cursor.fetchone()[0]
        print("Table name: "+table_name, end="")
        
        if p_record_type=='CSV' :
            key_column='seq_no'
        else:
            key_column='record_type'

        inserted_row_count = 0
        err_msg = cursor.var(str)
        exe_status =  cursor.var(str)
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'RRRRMMDD'")
        #cursor.callproc("pkg_rk_core.ins_base", ['Job', table_name, clb, session_id, err_msg, exe_status])
        insert_sql="""
            DECLARE
                l_no_of_rows NUMBER;
                l_error_status varchar2(4000);
                l_error_message varchar2(4000);
                l_error_json_clob    CLOB := EMPTY_CLOB ( );
                l_error_json        JSON;
                l_all_error_keys    json_list;
                l_all_error_values    json_list;                
            Begin
                DELETE FROM rk_json_gtt
                WHERE    session_id = :SESS_ID AND table_name = :TABLE_NAME AND logged_in_user_id = :LOGGED_IN_USER;
                
                --temp need to remove
                --execute immediate 'truncate table rk_error_log';
                --execute immediate 'truncate table monitor_rk' ;
                --execute immediate 'truncate table rk_json_gtt ';
                
                
                INSERT INTO rk_json_gtt ( rec_id, session_id, json_data, logged_in_user_id, creation_date, table_name )
                VALUES    
                (
                pkg_rk_core.generate_unique_id ( 'UTILS', :LOGGED_IN_USER ), :SESS_ID, :JSON_DATA, :LOGGED_IN_USER, SYSDATE, :TABLE_NAME
                );
                
                l_no_of_rows:=  SQL%ROWCOUNT;
                
                IF  l_no_of_rows>=1 THEN            
                    pkg_rk_core.ins (
                                   :TABLE_NAME 
                                 , :KEY_COLUMN
                                 , 'BULK'
                                 , :LOGGED_IN_USER
                                 , :SESS_ID 
                                 , l_error_json_clob
                                 , l_error_message
                                 , l_error_status
                                  );
                    :RETURN_MSG := l_error_message;--'Execution successful(1)';--
                    dbms_output.put_line( l_error_json_clob);
                    IF l_error_status <>  '0' THEN 
                        :RETURN_MSG := 'Rolled back-'||l_error_message;   
                    ELSE

                        DELETE FROM rk_json_gtt
                        WHERE    session_id = :SESS_ID AND table_name = :TABLE_NAME AND logged_in_user_id = :LOGGED_IN_USER;
                        
                        dbms_output.put_line( l_error_json_clob);
                        IF l_error_json_clob != EMPTY_CLOB ( ) THEN
                            l_error_json             := JSON ( l_error_json_clob );
                            l_all_error_keys         := l_error_json.get_keys ( );
                            l_all_error_values       := l_error_json.get_values ( );
                            l_error_message          := 'File DB load error-';
                            
                            FOR i IN 1 .. l_error_json.COUNT ( )
                            LOOP
                                l_error_message  := l_error_message || '\n' || l_all_error_keys.get ( i ).get_string ( ) || '::\n';
                                l_error_message  := l_error_message || '\n' || l_all_error_values.get ( i ).get_string ( );
                            END LOOP;
                              
                            l_error_status:='-1';                          
                        ELSE
                            NULL;
                            --the loading was absolutely fine
                        END IF;                        
                    END IF;
                    :RETURN_STAT := l_error_status;
                    :RETURN_MSG := l_error_message; 
                    
                ELSE   --if clob was not inserted itself
                    :RETURN_MSG := 'Rolled back-'||l_error_message;                
                END IF;
                
            END;
        """
        global file_json_data
        cursor.setinputsizes(JSON_DATA = cx_Oracle.CLOB)
        cursor.execute(insert_sql, SESS_ID=session_id, TABLE_NAME=table_name, LOGGED_IN_USER='Job', JSON_DATA=file_json_data, KEY_COLUMN=key_column, RETURN_STAT=exe_status, RETURN_MSG=err_msg )
        if exe_status == exe_status :
            print(', Status: '+ str(exe_status.getvalue()), end="")
        else:
            return FAILED
        
        #print(err_msg.getvalue())
        #print(exe_status.getvalue())
        
        if err_msg == err_msg :
            print(', '+ str(err_msg.getvalue()), end="")
            #print( err_msg.getvalue().find( 'Successful'), end="")
            if ( str(err_msg.getvalue()).find( 'Successful')) > 0:
                inserted_row_count=err_msg.getvalue()[(err_msg.getvalue().index('(')+1) : err_msg.getvalue().index(')')]
                #print('inserted_row_count:'+str(inserted_row_count), end="")
            global additional_info
            additional_info = additional_info+file_record_type+":"+str(inserted_row_count ) +", "
        else:
            return FAILED
        #check if the row counter calculated while creating the JSON matched with this
        global row_counter
        #print('inserted_row_count:'+str(inserted_row_count)+ ', row_counter:'+str(row_counter))
        global txn_processed_so_far  
        global partial_commit_done  

        if int(inserted_row_count) != int(row_counter):
           print(", Count in file and non-of-rows-inserted didn't match", end="")
           return FAILED
        else:
            txn_processed_so_far = int(txn_processed_so_far ) + int(inserted_row_count)
            if  txn_processed_so_far>10000 :
                con.commit()
                txn_processed_so_far=0
                #this is done to prevent hanging due to lesser size of redo log.
                partial_commit_done = "Y"
                print(" [Note: Partial commit done!!!]", end="")
            else:
                print(" [Record inserted so far:{}]".format(txn_processed_so_far), end="")
                

    except cx_Oracle.DatabaseError as ex:
        print ('***DB process error in insert_json_to_DB***')
        error, = ex.args    
        print ('Error.message = '+ error.message)
        return FAILED
    cursor.close()      
    return SUCCESS


#-------------------------------- 
#--------------------------------
#main 
#--------------------------------
#--------------------------------

print('*************Program starts********************')
current_time = arrow.now().format('MM/DD/YYYY HH:mm')
print(current_time)
print("------------------------------------------------")

lines = []
file_json_data =""
row_counter=0
session_id = random.randint(0,99999999)
print("Session_id: "+str(session_id), end="")
print(" ")
additional_info="" 
vendor_counter = 0
no_of_in_files = 0;
last_vendor_name = ""
txn_processed_so_far = 0
partial_commit_done = "N"
flg_send_email ='N'
flg_failed ='N'
excel_sheet_counter = 0
ora = cf.Oracle()
env = util.getEnv()
###################################
#BE CAREFUL TO CHANGE THIS IN THE PRODUCTION
###################################
con = ora.getConnection(env)
###################################
#BE CAREFUL TO CHANGE THIS IN THE PRODUCTION
###################################

#EBCDIC to ASCII equivalent conversion
ebcdic_postive = dict({'{': '0' ,'A': '1','B': '2','C': '3' ,'D': '4','E': '5','F': '6','G': '7','H': '8','I': '9'})
ebcdic_negitve = dict({'}': '0' ,'J': '1','K': '2','L': '3' ,'M': '4','N': '5','O': '6','P': '7','Q': '8','R': '9'});  

#reading the inbound file starts
email_body="""<html>
            <head><style type='text/css'>tr{line-height: 18px;}td{font-family:Tahoma; font-size:9pt;}th{font-family:Tahoma; font-size:9pt;background-color:#003366;font-weight: normal; color:white;}</style></head><body>"""
email_body= email_body+"<font face='verdana' size ='2'> Status of files processed by RisKonnect  </br></br>"
email_body= email_body+"<table border='2' cellspacing='2' cellpadding='1'>"
email_body= email_body+add_tr("<th>Sr#</th><th>File Name</th><th>Load ID</th><th>Status</th><th>Additional Info</th>")

#let's see how many vendors are there
vendor_data = get_vendor_data()

for vendors in vendor_data :

    email_body= email_body+ add_tr( add_td("<b>Vendor : </b>"+ vendors["VENDOR_NAME"]+"l:::M", p_col_span=5) )
    email_body_row=""
    
    if no_of_in_files==0 and vendor_counter>0 :
        print("\tNo files present for " + last_vendor_name+" : "+ str(no_of_in_files))      
        
    last_vendor_name=vendors["VENDOR_NAME"]
        
    no_of_in_files=0
    vendor_counter=vendor_counter+1   
    print(" ") 
    print("Processing for VENDOR =  " + vendors["VENDOR_NAME"], end="") 
    print(", ID:  " +str( vendors["CONF_ID"])+", File Type: "+ vendors['FILE_TYPE'] + ", Encoding: "+vendors['FILE_CODING'], ", Date Format: "+vendors['DATE_FORMAT'])
        
    vendor_record_types = []
    vendor_record_types = get_record_types( vendors["CONF_ID"])
    print("\tMaster data of record types for this vendor: ", end="")
    print( *vendor_record_types, sep=",")
    
    print("\tReading the inbound files...")
    #each vendor will have a dedicated folder, one folder would contain file for that vendor only
    for in_dirpath, in_dirnames, in_filenames in os.walk(vendors["INBOUND_PATH"]):
        if not in_filenames:
            continue
        in_files = fnmatch.filter(in_filenames, vendors["WILD_CARD"])
        #reading the file matching desired pattern
        if in_files:
            for in_file in in_files:
                print("\t---")
                partial_commit_done ="N"
                print('\tProcessing started for file:    '+in_file)            
                in_full_file_path = vendors["INBOUND_PATH"]+in_file
    
                #to be generated per file
                load_id=genLoadID()
                print("\tLoad ID:    {}".format(load_id))

                ret_val = add_load_details(in_file, vendors["VENDOR_NAME"], INPROCESS)
                if ret_val!=SUCCESS:
                    print("\t***ERROR: Load ID generation failed for this file skipping this and proceeding for the next")
                    continue
                
                successfully_processed_count = 0 
                no_of_in_files=no_of_in_files + 1   
                flg_failed ='N'             
                #------------------------------------------------
                #FWF portion
                #------------------------------------------------
                if vendors['FILE_TYPE'] == 'FWF':
                    randomString = genRandomString()                
                    fileName_rk = ''
                    
                    no_of_temp_files=0;
                    record_type=[]
                    
                    email_body_row = add_td(str(no_of_in_files))+add_td(in_file)+add_td(str(load_id))
    
                    #Removing half done files if any- of this file...
                    for hgx in glob.glob(staging_path+"*"+ in_file + "*.rk"):
                        os.remove(hgx)
                    print("\tSplitting main file, based on record type found in the first "+str(vendors["ROW_IDENTIFIER_LENGTH"])+" characters...")                    
                    with open(in_full_file_path) as f:
                        for k,v in groupby(f,lambda x: x[:vendors["ROW_IDENTIFIER_LENGTH"]]):        
                            fileName_rk=staging_path+format(k) + "_" + in_file + "_" + randomString + ".rk"   
                            #print("file name created:"+fileName_rk)     
                            with open( fileName_rk,"a") as f1: 
                                if k not in record_type: 
                                    record_type.append(k)                
                                f1.writelines(v)
        
                    additional_info="" 
                    print("\tRecord types found - in the file:" +",".join(record_type ))                
                       
                    for dirpath, dirnames, filenames in os.walk(staging_path):
                        if not filenames:
                            continue
                        
                        rk_temp_files = fnmatch.filter(filenames, temp_file_pattern)
                        
                        if rk_temp_files:
                            for file in rk_temp_files:                           
                                only_file_name=file                            
                                file_record_type = only_file_name[0:vendors["ROW_IDENTIFIER_LENGTH"]]           
                                if file_record_type in vendor_record_types:                                
                                    full_file_name = '{}/{}'.format(dirpath, file) 
                                    lines=[] 
                                    

                                    #ret_val = add_to_list(full_file_name, vendors["CONF_ID"], file_record_type)
                                    ret_val = create_json_from_fwf(full_file_name, vendors["CONF_ID"], file_record_type)
                                    
                                    if ret_val == SUCCESS:
                                        no_of_temp_files = no_of_temp_files+1  
                                        #additional_info = additional_info+file_record_type+":"+str(count(lines)  ) +", "                       
                                        print("\t"+str(no_of_temp_files)+". Processing for '"+file_record_type+"'...", end="")
                                        print (" JSON formed: "+ret_val+'('+str(row_counter)+')', end="")
                                        print(' | DB operation ...', end="")
                                        if file_json_data:
                                            #ret_val=insert_DB(in_file, vendors["CONF_ID"], file_record_type)
                                            ret_val=insert_json_to_DB(in_file, vendors["CONF_ID"], file_record_type)
                                            print (", File Status: "+ret_val)
                                            if ret_val== SUCCESS:
                                                successfully_processed_count=successfully_processed_count+1
                                            else:
                                                print("\tInsert into "+file_record_type+" failed***")
                                                flg_failed='Y' 
                                    else:
                                        print('\t***Error in creating jSON form FWF***', end="")
                                        flg_failed='Y'
                                        
                    print("\tCheck: Temp --> created:{}".format(no_of_temp_files)+", processed:{}".format(successfully_processed_count))  
                    #end of for loop to process split files of a file 
                #End of processing for FWF files                 
                
                #------------------------------------------------
                #CSV portion
                #------------------------------------------------                     
                elif vendors['FILE_TYPE']=='CSV':
                    #CSV Files processing starts
                    file_record_type = vendors['FILE_TYPE']
                    
                    no_of_temp_files = 0 #just to make use of the common code 
                    successfully_processed_count = 0 #just to make use of the common code
                    email_body_row = add_td(str(no_of_in_files))+add_td(in_file)+add_td(str(load_id))
                    additional_info="" 
                    
                    print("\tCreating JSON...", end="")
                    lines=[]
                    ret_val = create_json_from_csv (in_full_file_path, vendors["CONF_ID"], file_record_type)
                    print (ret_val+'('+str(row_counter)+')', end="")
                    if ret_val == SUCCESS:
                        no_of_temp_files = no_of_temp_files + 1
                        print(' | DB operation ...', end="")
                        ret_val = insert_json_to_DB (in_full_file_path,  vendors["CONF_ID"], file_record_type)                        
                                                
                        print (", File Status: "+ret_val)
                        if ret_val == SUCCESS:
                            successfully_processed_count = 1
                        else:
                            print("\tInsert into "+file_record_type+" failed***")   
                            flg_failed='Y'                     
                    else:
                        print("\tCreating JSON from "+vendors['FILE_TYPE']+" failed***")  
                        flg_failed='Y'                  
                #------------------------------------------------
                #Excel portion
                #------------------------------------------------                     
                elif vendors['FILE_TYPE']=='EXCEL':
                    #Excel Files processing starts
                    excel_sheet_counter = 0
                    for file_record_type in vendor_record_types: 
                        #file_record_type = vendors['FILE_TYPE']
                        
                        no_of_temp_files = 0 #just to make use of the common code 
                        successfully_processed_count = 0 #just to make use of the common code
                        email_body_row = add_td(str(no_of_in_files))+add_td(in_file)+add_td(str(load_id))
                        additional_info="" 
                        
                        print("\tCreating EXCEL...", end="")
                        lines=[]
                        ret_val = create_json_from_excel (in_full_file_path, vendors["CONF_ID"], file_record_type, excel_sheet_counter)
                        print (ret_val+'('+str(row_counter)+')', end="")
                        if ret_val == SUCCESS:
                            no_of_temp_files = no_of_temp_files + 1
                            print(' | DB operation ...', end="")
                            ret_val = insert_json_to_DB (in_full_file_path,  vendors["CONF_ID"], file_record_type)                        
                                                    
                            print (", File Status: "+ret_val)
                            if ret_val == SUCCESS:
                                successfully_processed_count = 1
                            else:
                                print("\tInsert into "+file_record_type+" failed***")   
                                flg_failed='Y'                                 
                                       
                        else:
                            print("\tCreating JSON from "+vendors['FILE_TYPE']+" failed***")  
                            flg_failed='Y'     
                            
                        excel_sheet_counter = excel_sheet_counter + 1
                else:
                    print("Files type not yet configured...Contact support!!!")
                    flg_failed='Y'
                    
                #End of processing for CSV Files  
                
                #---------------------------------------------------------------
                #common code for file level commit/roll-back/load details update
                #---------------------------------------------------------------  
                
                #update whether or not the file was successful
                if flg_failed == 'Y' :                
                    ret_val = add_load_details(in_file, vendors["VENDOR_NAME"], FAILED )
                else:
                    ret_val = add_load_details(in_file, vendors["VENDOR_NAME"], SUCCESS)
                
                if ret_val!=SUCCESS or flg_failed == 'Y':
                    con.rollback()
                    print("\t***ERROR: Load ID update failed for this file skipping this and proceeding for the next")
                    continue              

                    
                if successfully_processed_count == no_of_temp_files and flg_failed == 'N':
                    email_body_row=email_body_row+add_td("Success")+add_td(additional_info.rstrip(', '))
                    txn_processed_so_far=0
                    #shutil.move(in_full_file_path ,  vendors["ARCHIVE_PATH"]+in_file)
                    dst_file=vendors["ARCHIVE_PATH"]+in_file
                    if os.path.exists(dst_file):
                        # in case of the src and dst are the same file
                        if os.path.samefile(in_full_file_path, dst_file):
                            continue
                        os.remove(dst_file)
                    shutil.move(in_full_file_path ,  vendors["ARCHIVE_PATH"]+in_file)                        
                    print("\tCommitted...")
                    flg_send_email='Y'
                    con.commit()                    
                else:
                    email_body_row=email_body_row+add_td("Failed")+add_td("")
                    #in case partial roll-back was done then remove the entries already committed,
                    #based on load id and record type
                    con.rollback()
                    if partial_commit_done == "Y":
                        rollback_partially_committed_file (vendors["CONF_ID"], load_id)
                        con.commit()
                    
                    txn_processed_so_far=0
                    
                    shutil.move(in_full_file_path ,  vendors["FAILED_PATH"]+in_file)
                    print("\tRolled back...") 
                    flg_send_email='Y'  
                
                email_body_row=add_tr(email_body_row) +"\n"               
                email_body=email_body+email_body_row
                email_body_row=''                
                
                print('\tProcessing finished for file:    '+in_file) 
                
                #end if common code for file level commit/roll-back/load details update                                
            #this is end of block for loop iterating all the files matching the wild card mentioned in the vendor details table
        print(" ")
        #end of directly reading FOR loop for - if files found
    email_body=email_body.replace("l:::M","&nbsp&nbsp&nbsp <u>No of file(s) present:" +str(no_of_in_files)+"</u>")
    #vendor for loop ends
email_body=email_body+"</table></br></br></br>Regards,</br>Treasury - IT</font></br></br></font></body></html>"            
#print(email_body)
print("---")
if flg_send_email == 'Y' :
    util.sendEmail("RisKonnect job report at: "+str(current_time), email_body);
else:
    print("No files present..Exiting program")
ora.closeConnection()
print("------------------------------------------------")
print(arrow.now().format('MM/DD/YYYY HH:mm'))
print('*************Program ends********************')


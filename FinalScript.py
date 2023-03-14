#importing all the important modules used in script
import snowflake.connector
import re
from datetime import datetime

#connecting to snowflake
conn = snowflake.connector.connect(user='vaivishal',password='Deloitte@123',account='srb80301.us-east-1',warehouse='COMPUTE_WH',database='DATAMART')
cursor = conn.cursor()


#This is main function where we fill just pass the name of the error as an argument and it will run all the rules for that perticular error
def ruleSQL(ruleName):
    
    #This line will make a query something like: SELECT * FROM DATAMART.TMSIS_STG.T_TMSIS_RULES_REF_TABLE WHERE ERROR_TIER_NAME = 'Formatting Error'
    #{ruleName} will be replaced with the ruleName that will be passed as argument to this funtion
    SQL = f"SELECT * FROM DATAMART.TMSIS_STG.T_TMSIS_RULES_REF_TABLE WHERE ERROR_TIER_NAME = '{ruleName}'"

    #executing the SQL generated in last line
    df = cursor.execute(SQL).fetch_pandas_all()

    #checking if the ruleName passed as argument is 'Formatting Error'
    if ruleName == 'Formatting Error':
        #looping through each rule for formatting error in "T_TMSIS_RULES_REF_TABLE" table 
        for i in range(len(df)):
            #fetching the record of perticular columns required in dataLength() funtion from T_TMSIS_RULES_REF_TABLE table
            rulenumber = df.loc[i,"RULE_ID"]
            tablename = df.loc[i,"TABLE_NAME"]
            columnname = df.loc[i,"COLUMN_NAME"]
            columnlength = df.loc[i,"COLUMN_LENGTH"]
            datatype = df.loc[i,"DATA_TYPE"]
            main_table_condition = [df.loc[i, "MAIN_TABLE_CONDITION"]]
            joining_table_condition = [df.loc[i, "JOINING_TABLE_CONDITION"]]
            isJoining = df.loc[i,"IS_JOIN"]
            JOINING_TABLE = df.loc[i,"JOINING_TABLE"]
            JOINING_TABLE_COLUMNS1 = [df.loc[i,"JOINING_TABLE_COLUMNS1"]]
            JOINING_TABLE_COLUMNS2 = [df.loc[i,"JOINING_TABLE_COLUMNS2"]]
            JOINING_TYPE = df.loc[i,"JOINING_TYPE"]
            recordId = df.loc[i,"RECORD_ID"]
            #executing the dataLength funtion for each rule
            dataLength(rulenumber,recordId,tablename,columnname,columnlength,datatype,main_table_condition,joining_table_condition,isJoining,JOINING_TABLE,JOINING_TABLE_COLUMNS1,JOINING_TABLE_COLUMNS2,JOINING_TYPE)
    
    #checking if the ruleName passed as argument is 'File specification error'
    elif ruleName == 'File specification error':
        #looping through each rule for formatting error in "T_TMSIS_RULES_REF_TABLE" table 
        for i in range(len(df)):
            #fetching the record of perticular columns required in fileSpeciaficationError() funtion from T_TMSIS_RULES_REF_TABLE table
            columnname = [df.loc[i,"COLUMN_NAME"]]
            rulenumber = df.loc[i,"RULE_ID"]
            tablename = df.loc[i,"TABLE_NAME"]
            #executing the fileSpeciaficationError funtion for each rule
            fileSpeciaficationError(rulenumber,tablename,columnname)

    #checking if the ruleName passed as argument is 'Missing Value Error'
    elif ruleName == 'Missing Value Error':
        #looping through each rule for formatting error in "T_TMSIS_RULES_REF_TABLE" table
        for i in range(len(df)):
            #fetching the record of perticular columns required in missing_value() funtion from T_TMSIS_RULES_REF_TABLE table
            rulenumber = df.loc[i,"RULE_ID"]
            tablename = df.loc[i,"TABLE_NAME"]
            columnname = df.loc[i,"COLUMN_NAME"]
            columnlength = df.loc[i,"COLUMN_LENGTH"]
            datatype = df.loc[i,"DATA_TYPE"]
            main_table_condition = [df.loc[i, "MAIN_TABLE_CONDITION"]]
            joining_table_condition = [df.loc[i, "JOINING_TABLE_CONDITION"]]
            isJoining = df.loc[i,"IS_JOIN"]
            JOINING_TABLE = df.loc[i,"JOINING_TABLE"]
            JOINING_TABLE_COLUMNS1 = [df.loc[i,"JOINING_TABLE_COLUMNS1"]]
            JOINING_TABLE_COLUMNS2 = [df.loc[i,"JOINING_TABLE_COLUMNS2"]]
            JOINING_TYPE = df.loc[i,"JOINING_TYPE"]
            validationLogic = df.loc[i,"VALIDATION_LOGIC"]
            is_condition = df.loc[i,"IS_CONDITION"]
            print(rulenumber)
            #executing the fileSpeciaficationError funtion for each rule
            missing_value(tablename, columnname, validationLogic ,datatype, is_condition, isJoining, JOINING_TYPE, JOINING_TABLE, JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2, main_table_condition, joining_table_condition)

    #checking if the ruleName passed as argument is 'Value Error'
    elif ruleName == 'Value Error':
        #looping through each rule for formatting error in "T_TMSIS_RULES_REF_TABLE" table
        for i in range(len(df)):
            #fetching the record of perticular columns required in valueError() funtion from T_TMSIS_RULES_REF_TABLE table
            rulenumber = df.loc[i,"RULE_ID"]
            tablename = df.loc[i,"TABLE_NAME"]
            columnname = df.loc[i,"COLUMN_NAME"]
            columnlength = df.loc[i,"COLUMN_LENGTH"]
            datatype = df.loc[i,"DATA_TYPE"]
            main_table_condition = [df.loc[i, "MAIN_TABLE_CONDITION"]]
            joining_table_condition = [df.loc[i, "JOINING_TABLE_CONDITION"]]
            isJoining = df.loc[i,"IS_JOIN"]
            JOINING_TABLE = df.loc[i,"JOINING_TABLE"]
            JOINING_TABLE_COLUMNS1 = [df.loc[i,"JOINING_TABLE_COLUMNS1"]]
            JOINING_TABLE_COLUMNS2 = [df.loc[i,"JOINING_TABLE_COLUMNS2"]]
            JOINING_TYPE = df.loc[i,"JOINING_TYPE"]
            validationLogic = df.loc[i,"VALIDATION_LOGIC"]
            is_condition = df.loc[i,"IS_CONDITION"]
            recordId = df.loc[i,"RECORD_ID"]
            print(rulenumber)
            #executing the fileSpeciaficationError funtion for each rule
            valueError(rulenumber,recordId,tablename, validationLogic,main_table_condition,columnname,isJoining, JOINING_TABLE,JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2, JOINING_TYPE, joining_table_condition,is_condition)

    else:
        print("Rule name Entered does not exist")
        return 

############################ IMPORTANT SMALL FUNCTIONS USED IN RULES SCRIPT IMPLEMENTATION #######################

def format_snowflake_query(query):
    """
    Formats a Snowflake SQL query string by removing unnecessary white spaces and line breaks.
    """
    query = query.strip()  # Remove leading/trailing white spaces
    query = ' '.join(query.split())  # Replace multiple white spaces with a single space
    return query


#used in generate_sql() function to split the value in list
def split_list_values(lst):
    new_lst = []
    for item in lst:
        new_lst.extend(item.split(','))
    return new_lst


# will make and return query with all the joins
def generate_sql(tablename, JOINING_TABLE, JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2, JOINING_TYPE, main_table_condition, joining_table_condition):
    join_clauses = []
    JOINING_TABLE_COLUMNS1 = split_list_values(JOINING_TABLE_COLUMNS1)
    JOINING_TABLE_COLUMNS2 = split_list_values(JOINING_TABLE_COLUMNS2)
    for col1, col2 in zip(JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2):
        join_clauses.append(f"{tablename}.{col1} = {JOINING_TABLE}.{col2}")
    join_clause = " AND ".join(join_clauses)
    query = f"SELECT {tablename}.ICN_ORIG FROM DATAMART.TMSIS_STG.{tablename} {JOINING_TYPE} DATAMART.TMSIS_STG.{JOINING_TABLE} ON {join_clause} WHERE {main_table_condition[0]} AND {joining_table_condition[0]}"
    return query

# will make and return query with all the joins
def generate_sql2(tablename, JOINING_TABLE, JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2, JOINING_TYPE, main_table_condition, joining_table_condition):
    join_clauses = []
    JOINING_TABLE_COLUMNS1 = split_list_values(JOINING_TABLE_COLUMNS1)
    JOINING_TABLE_COLUMNS2 = split_list_values(JOINING_TABLE_COLUMNS2)
    for col1, col2 in zip(JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2):
        join_clauses.append(f"{tablename}.{col1} = {JOINING_TABLE}.{col2}")
    join_clause = " AND ".join(join_clauses)
    if len(joining_table_condition[0]) == 0:
        query = f"SELECT {tablename}.ICN_ORIG FROM DATAMART.TMSIS_STG.{tablename} {JOINING_TYPE} DATAMART.TMSIS_STG.{JOINING_TABLE} ON {join_clause} WHERE {main_table_condition[0]}"
    else:
        query = f"SELECT {tablename}.ICN_ORIG FROM DATAMART.TMSIS_STG.{tablename} {JOINING_TYPE} DATAMART.TMSIS_STG.{JOINING_TABLE} ON {join_clause} WHERE {main_table_condition[0]} AND {joining_table_condition[0]}"
    return query

# cwill check if decimal given is in correct format or not
def is_decimal(num):
    try:
        float(num)
        return True
    except ValueError:
        return False
    

# date check function
def date_check(current_data, date_format):
    try:
        datetime.strptime(current_data, date_format)
        print('Date is in correct format.')
    except ValueError:
        print('Date is not in correct format. Insert data in error table')
        
#amount check function
def amount_check(amount_regex, current_data):
    if re.match(amount_regex, current_data):
        print('The string represents an amount.')
    else:
        print('The string does not represent an amount.')



############################ FILE SPECIFICATION IMPLEMENTATION ###################################################

def fileSpeciaficationError(rulenumber, tablename, columnName):
    try:
        columns = ','.join(columnName)
        #creating the query to check for negative scenario
        SQL = f"SELECT {columns}, COUNT(*) FROM DATAMART.TMSIS_STG.{tablename} GROUP BY {columns} HAVING COUNT(*) > 1"

        #formatting the query to remove extra space or line break
        SQL = format_snowflake_query(SQL)

        # OPTIONAL: printing the query just to see what type of query is geting genearated for what rule number
        print(rulenumber)
        print(SQL)

        #executing the query
        df = cursor.execute(SQL).fetch_pandas_all()

        #looping throught all the data that is generated through negative scenario SQL above and inserting them in ERROR table
        for i in range(len(df)):
            print(f"Error generated for {rulenumber}")()
            # Change the role to app role
            conn.cursor().execute("USE ROLE APP_ROLE")
            # Perform insert operation
            conn.cursor().execute(f"INSERT INTO DATAMART.TMSIS.T_TMSIS_ERROR_REPORT (BATCH_ID, RUN_ID,RULE_ID, RECORD_ID,ERROR_TABLE_NAME,ERROR_RECORD_KEY) VALUES ('1966','12515', '{rulenumber}' , 'CRX00002', '{tablename}', 'ABCD')")
            
    # catching the error here without breaking the flow of code. This will show the rule number for which we are getting error along with error name        
    except Exception as e:
        print('--------------------------ERROR----------------------------------')
        print(rulenumber)
        print(type(e).__name__,e)


########################################### Formatting Error ###################################################


def dataLength(rulenumber,recordId,tablename,columnName,columnlength,datatype,main_table_condition,joining_table_condition,isJoining,JOINING_TABLE,JOINING_TABLE_COLUMNS1,JOINING_TABLE_COLUMNS2,JOINING_TYPE):
        try:
            # making SQL where table name is T_FILE_HEADER_RECORD
            if('T_FILE' in tablename):
                SQL = f"SELECT {columnName} from DATAMART.TMSIS_STG.{tablename} WHERE RECORD_ID = '{recordId}'"
            # if there is join then this will get executed and this will go into generate_sql() funtion to genearate the query with all the joins
            elif isJoining == 'Y':
                print("Before fail")
                SQL = generate_sql(tablename, JOINING_TABLE, JOINING_TABLE_COLUMNS1, JOINING_TABLE_COLUMNS2, JOINING_TYPE, main_table_condition, joining_table_condition)
            # this will get executed if we don't have any condtion
            elif len(main_table_condition[0]) == 0:
                SQL = f"SELECT {columnName} FROM DATAMART.TMSIS_STG.{tablename}"
            #this will get executed if there is condition but no joining
            else:
                SQL = f"SELECT {columnName} FROM DATAMART.TMSIS_STG.{tablename} WHERE {main_table_condition[0]}"
            
            # OPTIONAL: Checking the SQL getting genearted for perticular rule
            print(rulenumber)
            print(SQL)

            #formatting the query to remove extra space or line break
            SQL = format_snowflake_query(SQL)

            #executing the SQL and fetincg all data in dataframe
            df = cursor.execute(SQL).fetch_pandas_all()

            #looping through each data
            for i in range(len(df)):
                #feting data for the column name mention in rule which needs to be tested
                curr_data = df.loc[i, f"{columnName}"]

                #if data is null or blank
                if curr_data is None or curr_data == '' or curr_data == 'NULL':
                    print(f"Data is null insert it into error table. Data is {curr_data}")
                    continue
                
                #if the given check is for decimal then it will go inside this if condition
                elif datatype == 'DECIMAL':
                    #if data is 0 then continue
                    if curr_data == '0':
                        continue
                    
                    # Feting the length of column mentioned in validation logic
                    before_decimal = int(str(columnlength).split('.')[0])
                    after_decimal = int(str(columnlength).split('.')[1])

                    #feting the length of current data
                    i_before = len(str(curr_data).split('.')[0])
                    i_after = len(str(curr_data).split('.')[1])

                    #checking if lenght of current data > length mentioned in validation logic
                    if i_before > before_decimal or i_after > after_decimal and not is_decimal(curr_data):
                        # Change the role to app role
                        conn.cursor().execute("USE ROLE APP_ROLE")
                        # Perform insert operation
                        conn.cursor().execute(f"INSERT INTO DATAMART.TMSIS.T_TMSIS_ERROR_REPORT (BATCH_ID, RUN_ID,RULE_ID, RECORD_ID,ERROR_TABLE_NAME,ERROR_RECORD_KEY) VALUES ('1966','12515', '{rulenumber}' , 'CRX00002', '{tablename}', 'ABCD')")
                
                # date check
                elif datatype == 'DATE':
                    #date should be in format of YYYYMMDD
                    format = "%Y%m%d"
                    try:
                        res = bool(datetime.strptime(curr_data, format))
                    except ValueError:
                        res = False

                    # If date is not in YYYYMMDD then res = False then insert it into error table
                    if res == False:
                        # Change the role to app role
                        conn.cursor().execute("USE ROLE APP_ROLE")

                        # Perform insert operation
                        conn.cursor().execute(f"INSERT INTO DATAMART.TMSIS.T_TMSIS_ERROR_REPORT (BATCH_ID, RUN_ID,RULE_ID, RECORD_ID,ERROR_TABLE_NAME,ERROR_RECORD_KEY) VALUES ('1966','12515', '{rulenumber}' , 'CRX00002', '{tablename}', 'ABCD')")
                
                # string check
                elif datatype == 'STRING':
                    # If lenght of string > lenght 
                    if(len(curr_data) > int(columnlength)):
                        # Change the role to app role
                        conn.cursor().execute("USE ROLE APP_ROLE")
                        # Perform insert operation
                        conn.cursor().execute(f"INSERT INTO DATAMART.TMSIS.T_TMSIS_ERROR_REPORT (BATCH_ID, RUN_ID,RULE_ID, RECORD_ID,ERROR_TABLE_NAME,ERROR_RECORD_KEY) VALUES ('1966','12515', '{rulenumber}' , 'CRX00002', '{tablename}', 'ABCD')")
        
        # catching the error here without breaking the flow of code. This will show the rule number for which we are getting error along with error name        
        except Exception as e:
            print('------------ERROR-------------------------')
            print(rulenumber)
            print(type(e).__name__,e)


########################################### Missing Value Error ###################################################


def missing_value(table_name, column_name, VALIDATION_LOGIC, data_type, is_condition, is_join, joining_type, joining_table, joining_table_columns1, joining_table_columns2, main_table_condition, joining_table_condition):
    try:
        #defining the regular expession for date and amiunt check
        date_format = '%Y-%m-%d'
        amount_regex = r'^\d+(\.\d{2})?$'

        #checking if the validation logic hac 'has a null value' sentence in it. If yes then go in this condition
        if 'has a null value' in VALIDATION_LOGIC:
            # if there is not joining condition then go inside this
            if is_join == 'N':
                query = f"SELECT {column_name} FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]} AND {column_name} IS NULL OR {column_name} = '0.00'"
            # if there is joining condition then go to generate_sql2() funtion that will create the query with all the joins
            elif is_join == 'Y':
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
                query += f" AND {joining_table}.{column_name} IS NOT NULL OR {joining_table}.{column_name} = '0.00'"
            # OPTIONAL: Print the query
            print(query)

            # formatting and executing the query
            query = format_snowflake_query(query)
            df = cursor.execute(query).fetch_pandas_all()

            # Fetich all the data we get from negative scenario and putting them in ERROR table
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"] 
                print(f"Insert in error table. Data is {curr_data}")
        else:
            if is_join == 'N':
                if column_name == 'SERVICE_TRACKING_TYPE':
                    query = f"SELECT * FROM TMSIS_STG.{table_name} WHERE {main_table_condition[0]} AND {column_name} IS NOT NULL OR {column_name} NOT IN ('0', '00')"
                    print(query)
                    query = format_snowflake_query(query)
                    df = cursor.execute(query).fetch_pandas_all()
                    for i in range(len(df)):
                        curr_data = df.loc[i, f"{column_name}"]
                elif is_condition == 'N':
                    query = f"SELECT * FROM TMSIS_STG.{table_name} WHERE {column_name} IS NOT NULL"
                    print(query)
                    query = format_snowflake_query(query)
                    df = cursor.execute(query).fetch_pandas_all()
                    for i in range(len(df)):
                        curr_data = df.loc[i, f"{column_name}"]
                        if data_type == 'DATE':
                            #checking date from date_check() function. If data is not correct it will insert it in error table
                            date_check(curr_data, date_format)

                        elif data_type == 'AMOUNT':
                            #checking date from amount_check() function. If data is not correct it will insert it in error table
                            amount_check(amount_regex, curr_data)
                elif is_condition == 'Y':
                    query = f"SELECT * FROM TMSIS_STG.{table_name} WHERE {main_table_condition[0]} AND {column_name} IS NOT NULL"
                    print(query)
                    query = format_snowflake_query(query)
                    df = cursor.execute(query).fetch_pandas_all()
                    for i in range(len(df)):
                        curr_data = df.loc[i, f"{column_name}"]
                        if data_type == 'DATE':
                            date_check(curr_data, date_format)
                        elif data_type == 'AMOUNT':
                            amount_check(amount_regex, curr_data)
            elif is_join == 'Y':
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
                query += f" AND {joining_table}.{column_name} IS NOT NULL"
                print(query)
                query = format_snowflake_query(query)
                df = cursor.execute(query).fetch_pandas_all()
                for i in range(len(df)):
                    curr_data = df.loc[i, f"{column_name}"]
    # catching the error here without breaking the flow of code. This will show the rule number for which we are getting error along with error name        
    except Exception as e:
        print('----------------ERROR----------------')
        print(type(e).__name__,e)



####################################### Value Error ###########################################


def crossMapTable(data_element_name):
    data_element_name = data_element_name.replace("_", "-")
    query = f"SELECT TMSIS_CODE FROM DATAMART.TMSIS.T_TMSIS_CROSSMAP WHERE DATA_ELEMENT_NAME = '{data_element_name}'"
    print(f"Inside crossmap, query is: {query}")
    df = cursor.execute(query).fetch_pandas_all()
    codes = []
    for i in range(len(df)):
        curr_data = df.loc[i, f"TMSIS_CODE"]
        codes.append(curr_data)
    print(f"Code in crossmap: {codes}")
    return codes


def valueError(rulenumber,recordId,table_name, validation_logic,main_table_condition,column_name,is_join, joining_table,joining_table_columns1, joining_table_columns2, joining_type, joining_table_condition,is_condition):
    try:
        #check for | and *
        if('|' in validation_logic and '*' in validation_logic):
            print('Check for | and *')
            if('T_FILE' in table_name):
                query = f"SELECT {column_name} from DATAMART.TMSIS_STG.{table_name} WHERE RECORD_ID = '{recordId}' AND {column_name} IS NOT NULL  AND {column_name} LIKE '%|%' AND {column_name} LIKE '%*%'; "
            elif(is_join == 'N'):
                query = f"SELECT * FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]} AND {column_name} LIKE '%|%' AND {column_name} LIKE '%*%';"
            else:
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
            query = format_snowflake_query(query)
            print(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                if('*' in curr_data or '|' in curr_data):
                    print("Error will generate since * or | is present")

        #checking zip code
        elif("5_digit" in validation_logic and "9_digit" in validation_logic):
            print('zip code check')
            query = f"SELECT {column_name} FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]}"
            query = format_snowflake_query(query)
            zip_pattern = re.compile(r"^\d{5}(?:\d{4})?$")
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                if(re.match(zip_pattern,curr_data)):
                    print("Zip is in correct format so continue")
                    continue
                else:
                    print("Not correct format hence error")

        #check for SSN
        elif("5_digit" not in validation_logic and "9_digit" in validation_logic):
            print('check for SSN')
            query = f"SELECT {column_name} FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]}"
            query = format_snowflake_query(query)
            ssn_patern = re.compile(r"^\d{9}$")
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                if(re.match(ssn_patern,curr_data)):
                    print("SSN is in correct format so continue")
                    continue
                else:
                    print("Not correct format hence error")

        #check for email address
        elif('@' in validation_logic):
            print('check for email address')
            query = f"SELECT {column_name} FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]}"
            query = format_snowflake_query(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                if '@' not in curr_data:
                    print("Since @ was not there it will throw error")

        #check for greater than or equal to zero
        elif('greater than or equal to zero' in validation_logic):
            print('check for greater than or equal to zero')
            query = f"SELECT {column_name} FROM DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]}"
            query = format_snowflake_query(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                if curr_data < 0:
                    print("Will throw error")

        #check for NPI number
        elif('National Provider Identifier (NPI) number' in validation_logic):
            print('check for NPI number')
            if(is_join == 'Y'):
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
            else:    
                query = f"SELECT {column_name} from DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]};"
            query = format_snowflake_query(query)
            print(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                npi_query = f"SELECT PROVR_NPI FROM ADS_WY.T_PROVR_NPI_DIM WHERE PROVR_NPI = {curr_data}"
                npi_df = cursor.execute(npi_query).fetch_pandas_all()
                if(len(npi_df) == 0):
                    print("NPI value not present in T_PROVR_NPI_DIM table")


        #check for NDC number
        elif('National Drug Code (NDC) value' in validation_logic):
            print('check for NDC number')
            if(is_join == 'Y'):
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
            else:    
                query = f"SELECT {column_name} from DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]};"
            query = format_snowflake_query(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                npi_query = f"SELECT DRUG_NDC_CD FROM ADS_WY.T_DRUG_DIM WHERE PROVR_NPI = {curr_data}"
                npi_df = cursor.execute(npi_query).fetch_pandas_all()
                if(len(npi_df) == 0):
                    print("NDC value not present in T_DRUG_DIM table")


        #this will go in crossmap table and check if value is correct
        else:
            print('Crossmap check')
            if(is_join == 'Y'):
                query = generate_sql2(table_name, joining_table, joining_table_columns1, joining_table_columns2, joining_type, main_table_condition, joining_table_condition)
            else:
                if(is_condition == 'Y'):
                    query = f"SELECT {column_name} from DATAMART.TMSIS_STG.{table_name} WHERE {main_table_condition[0]}"
                else:
                    query = f"SELECT {column_name} from DATAMART.TMSIS_STG.{table_name}"
            query = format_snowflake_query(query)
            print(query)
            df = cursor.execute(query).fetch_pandas_all()
            for i in range(len(df)):
                curr_data = df.loc[i, f"{column_name}"]
                tmsis_cd_list = crossMapTable(column_name)
                if curr_data not in tmsis_cd_list:
                    print("Code in table is not prsent in crossmap hence error")
    except Exception as e:
        print('----------------------ERROR---------------------------')
        print(rulenumber)
        print(type(e).__name__,e)


##################################### ASKING FOR INPUT FROM HERE #########################################


print("Enter the Rule You want to execute. Available Rules are:")
print("\t1. Formatting Error")
print("\t2. File specification error")
print("\t3. Missing Value Error")
print("\t4. Value Error")

ruleName = input("Enter you response now : ")

ruleSQL(ruleName)

print("--------------------Run Complete--------------------")


#closing the connection
conn.close()
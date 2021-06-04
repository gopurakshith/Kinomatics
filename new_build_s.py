#from zip_draft_version import get_list_and_extract
#from draft_version import  extract_and_push
import os
from pathlib import Path
from zipfile import ZipFile
import pandas as pd 
import xml.etree.ElementTree as et 
import sys
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras

#Change this path according to the location of XML files
change_in_path = '/data/code_testing_2'

my_folders_list = os.listdir(change_in_path)

#Gets the list of folders in the current directory
def get_folder_list(my_folders_list):

    for each_folder in my_folders_list:
        if each_folder.endswith('.py') or each_folder == '__pycache__':
            pass
        else:
            #Passing the date folder --> 20191206
            date_input = each_folder
            basepath = Path(change_in_path +'/'+ date_input)  

            #Change this to the database table name
            table_name = 'screenings_2021' #Name of the table in PostgreSQL database
            csv_file_name = change_in_path +'/'+ date_input + '_draft_version_csv.csv'
            file_path =  change_in_path + '/'+date_input
            #Send the data to zip_draft_version
            get_list_and_extract(table_name, csv_file_name, basepath,date_input, file_path)
    
    return print('End of the Execution')


#Method to get the zip files and extract them

def get_list_and_extract(table_name, csv_file_name, basepath, date_input, file_path):

    #To get list of unzipped_files
    list_of_unzipped_files = []
    files_in_basepath = basepath.iterdir()
    for item in files_in_basepath:
        if item.is_file():
            b = str(item.name)  #Converting item.name to string format
        if b.endswith('.zip') : #and (b.endswith('chn.zip')!= True): 
            list_of_unzipped_files.append(b)
    print("List of unzipped files are: ")
    print(len(list_of_unzipped_files))

    #Unzip the files and get a list of '.XMl' files and 'S.XML' files
    itteration_count = 0
    for each_unzipped_file in list_of_unzipped_files:
        itteration_count += 1
        country_name = []
        file_name = []
        country_input = each_unzipped_file[7:10]
        country_name.append( country_input )
        print('Country code: {}'.format(country_input))
        # specifying the zip file name
        file_name = each_unzipped_file 
        file_path_name = file_path+'/'+file_name
        with ZipFile(file_path_name, 'r') as zip:
            #zip.printdir()
            zip.extractall()
        zip.close()

        #Find XML files and give it to extract_and_push_method
        list_of_s_xml_files = []
        list_of_xml_files = []

        unzipped_path = Path(change_in_path)
        files_in_basepath = unzipped_path.iterdir()
        for item in files_in_basepath:
            if item.is_file():
                a = str(item.name)  
                if a.endswith('.XML') or a.endswith('.xml'):      #To detect .XML files in the folders
                    list_of_xml_files.append(a)
                if a.endswith('S.XML') or a.endswith('S.xml'):    #To detect S.XML files in the folders
                    list_of_s_xml_files.append(a)
    
        #For loop to excute the extract_and_push function to all the file
        for each_file_to_be_extracted in list_of_s_xml_files:
            passing_file_path = each_file_to_be_extracted
            error_count = extract_and_push( passing_file_path, country_input, csv_file_name,date_input, table_name )
            print("Error count for file {}{} is {}".format(country_input, each_file_to_be_extracted, error_count))
        #removing the unzipped files
        for each_xml_file in list_of_xml_files:
            os.remove(each_xml_file)
    
    print('Iteration count: {}'.format(itteration_count))
    return print("End of current folder {}".format(date_input) ) 

def extract_and_push(xml_file_name, country_input, csv_file_name,date_input, table_name):
    erro_count = 0
    import pandas as pd 
    import xml.etree.ElementTree as et 
    xtree = et.parse(xml_file_name)           #"191207S.XML")
    xroot = xtree.getroot() 
    df_cols = ["movie_name", "movie_id", "theater_id", "show_date", "show_times_count","count_value","country_code", "week_date"]
    rows = []

    if country_input == 'chn':
        print('China')
        for node in xroot:
            space_count = 0
            s_country_code = country_input
            s_movie_name = node.find("movie_name").text if node.find('movie_name') is not None else ''

            #Integers
            s_movie_id = int(node.find("movie_id").text) if node.find('movie_id') is not None else 0
            s_theater_id = int(node.find("theater_id").text) if node.find('theater_id') is not None else 0

            #Date
            if node.find('show_date') is None:
                s_show_date = '0001-01-01' #'2000-01-01'
            elif node.find('show_date').text is None:
                s_show_date = '0001-01-01' #'2000-01-01'
            else:
                processing_text = node.find('show_date').text

                processing_text = processing_text.split(' ')
                processing_text = processing_text[0].split('/')
                c_year = processing_text[0]
                c_month = processing_text[1]
                c_day = processing_text[2]
                s_show_date = c_year + '-' + c_month + '-' + c_day

            '''temp_date = node.find("show_date").text if node.find('show_date') is not None else '0001-01-01'
            year = temp_date[:4]
            month = temp_date[4:6]
            day = temp_date[6:8]
            s_show_date = year + '-' + month + '-' + day'''
            #Date
            s_week_date = date_input[:4] + '-' + date_input[4:6] + '-' + date_input[6:8]

            #s_show_date = s_show_date[:10]
            bit = node.find("showtimes").text
            bit = bit[0:1]
            if (node.find("showtimes") is None) or (bit.isdigit() == False):
                s_show_times_count = ''
                space_count = 0 
            else:
                s_show_times_count = node.find("showtimes").text 
                s_show_times_count = s_show_times_count.replace(',','; ').replace('.','; ')
                space_count = s_show_times_count.count(';')
                s_show_times_count = s_show_times_count[:-2]
                
            rows.append( { "movie_name": s_movie_name, "movie_id": s_movie_id, "theater_id": s_theater_id, "show_date": s_show_date, "show_times_count":s_show_times_count, "count_value": space_count,"country_code":s_country_code, "week_date":s_week_date})
        #c_test = '/data/code_testing/china_test_draft_version_csv.csv'
        #print(c_test)
        out_df = pd.DataFrame(rows, columns = df_cols)
        #out_df.to_csv(c_test, header=True,index = False)

    else:
        for node in xroot:
            s_country_code = country_input
            s_movie_name = node.find("movie_name").text if node.find('movie_name') is not None else ''

            #Integers
            s_movie_id = int(node.find("movie_id").text) if node.find('movie_id') is not None else 0
            s_theater_id = int(node.find("theater_id").text) if node.find('theater_id') is not None else 0
            
            #Date
            temp2_date = node.find("show_date").attrib.get("date") if node.find('show_date').text is not None else '0001-01-01'
            year_2 = temp2_date[:4]
            month_2 = temp2_date[4:6]
            day_2 = temp2_date[6:8]
            s_show_date = year_2 + '-' + month_2 + '-' + day_2

            #Date
            w_year = date_input[:4]
            w_month = date_input[4:6]
            w_day = date_input[6:8]
            s_week_date = w_year + '-' + w_month + '-' + w_day
            #Code to include multiple show times count from multiple show date
            s = ''  #temp variable to store multiple show times count
            for child in node:
                if child.tag == 'show_date':
                    try:
                        s = s + child.find('showtimes').text if child.find('showtimes').text is not None else ''
                    except AttributeError:
                        erro_count+=1
                        s = s+''
            s_show_times_count = s
            space_count = 0
            s_show_times_count = s_show_times_count.replace(',','; ').replace('.','; ')
            space_count =  s_show_times_count.count(';')
            s_show_times_count = s_show_times_count[:-2]
            #space_count = 0 if space_count<1 else space_count = space_count

            rows.append( { "movie_name": s_movie_name, "movie_id": s_movie_id, "theater_id": s_theater_id, "show_date": s_show_date, "show_times_count":s_show_times_count, "count_value": space_count,"country_code":s_country_code, "week_date":s_week_date})
        out_df = pd.DataFrame(rows, columns = df_cols)

    out_df['movie_name'] = out_df['movie_name'].str.replace(',',';')

    conn_params_dic = {
        "host"      : 'Enter Name of the host',
        "database"  : "Enter Name of the database",
        "user"      : "Enter Database user name",
        "password"  : "Enter Database password"
    }

    def show_psycopg2_exception(err):
        err_type, err_obj, traceback = sys.exc_info()    
        line_n = traceback.tb_lineno    
        print ("\n psycopg2 ERROR:", err, "on line number:", line_n)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
        print ("\n extensions.Diagnostics:", err.diag)    
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")

    def connect(conn_params_dic):
        conn = None
        try:
            #print('Connecting to the PostgreSQL...........')
            conn = psycopg2.connect(**conn_params_dic)
            #print("Connection successfully..................")
        except OperationalError as err:
            show_psycopg2_exception(err)        
            conn = None
        return conn

    # Define function using copy_from_dataFile to insert the dataframe.
    def copy_from_dataFile(conn, df, table):
    #  Here we are going save the dataframe on disk as a csv file, load # the csv file and use copy_from() to copy it to the table
        csv_file_name = change_in_path+ date_input + '_draft_version_csv.csv'
        tmp_df = csv_file_name
        df.to_csv(tmp_df, header=False,index = False)
        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        try:
            cursor.copy_from(f, table, sep=",")
            conn.commit()
            print("Data inserted using copy_from_datafile() successfully....")
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            #os.remove(tmp_df)
            show_psycopg2_exception(error)
            cursor.close()
        f.close()
        os.remove(tmp_df)

    #Step 4: Perform main task
    # Connect to the database
    conn = connect(conn_params_dic)
    conn.autocommit = True
    copy_from_dataFile(conn, out_df , table_name)   # 2nd variable --> dataframes variable, # 3rd variable --> declare the database table name
    # Close the connection
    conn.close()

    return 0

get_folder_list(my_folders_list)


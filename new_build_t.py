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
            table_name = 'venues'    #Name of the table in PostgreSQL database
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
                if a.endswith('.XML') or a.endswith('.xml'):             #To detect .XML files in the folders
                    list_of_xml_files.append(a)
                if a.endswith('T.XML') or a.endswith('T.xml'):           #To detect T.XML files in the folders
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

    #Change required from here--> T.XML file
    df_cols = ["venue_id", "name", "screens", "state", "country", "city", "market", "closed_reason", "zip", "lat", "long", "type","file_date"]
    #df_cols = ["movie_name", "movie_id", "theater_id", "show_date", "show_times_count","count_value","country_code", "week_date"]
    rows = []

    for node in xroot:
        t_venue_id = int(node.find("theater_id").text) if node.find("theater_id") is not None else 0
        t_name = node.find("theater_name").text if node.find("theater_name") is not None else ""
        t_country = node.find("theater_country").text if node.find("theater_country") is not None else ""
        t_city = node.find("theater_city").text if node.find("theater_city") is not None else ""
        t_market = node.find("theater_market").text if node.find("theater_market") is not None else ""
        t_closed_reason = node.find("theater_closed_reason").text if node.find("theater_closed_reason") is not None else ""
        t_zip = node.find("theater_zip").text if node.find("theater_zip") is not None else ""
        t_lat = node.find("theater_lat").text if node.find("theater_lat") is not None else ""
        t_long = node.find("theater_lon").text if node.find("theater_lon") is not None else ""
        
        #New changes
        t_file_date = date_input[:4]+'-'+date_input[4:6]+'-'+date_input[6:]
        t_theater_state = node.find("theater_state").text if node.find("theater_state") is not None else ""
        t_theater_screens = int(node.find("theater_screens").text) if node.find("theater_screens") is not None else 0

        #For theater_type - list
        if node.find('theater_types') is None:
            t_types = ''
        else:
            x = node.find('theater_types').getchildren()
            if len(x)>=1:
                s = ''
                for child in node:
                    if child.tag == 'theater_types':
                        z = child.findall('type')
                        for i in z:
                            s = s + i.text + '; '
                        t_types = s[:-2]    
            else:
                t_types = ''

        rows.append({"venue_id":t_venue_id, "name":t_name, "screens":t_theater_screens, "state":t_theater_state,"country":t_country, "city":t_city, "market":t_market, "closed_reason":t_closed_reason, "zip":t_zip, "lat":t_lat, "long":t_long, "type":t_types, "file_date":t_file_date})
    out_df = pd.DataFrame(rows, columns = df_cols)

    #out_df['id'] = out_df['id'].str.replace(',',';')
    out_df['name'] = out_df['name'].str.replace(',',';')
    out_df['city'] = out_df['city'].str.replace(',',';')
    out_df['market'] = out_df['market'].str.replace(',',';')
    out_df['closed_reason'] = out_df['closed_reason'].str.replace(',',';')

    #Change required for T.XMl till here

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


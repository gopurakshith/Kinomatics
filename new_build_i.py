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
change_in_path = '/data/code_testing_2/'

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
            table_name = 'movies'    #Name of the table in PostgreSQL database
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
                if a.endswith('.XML') or a.endswith('.xml'):         #To detect .XML files in the folders
                    list_of_xml_files.append(a)
                if a.endswith('I.XML') or a.endswith('I.xml'):       #To detect I.XML files in the folders        
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
    xtree = et.parse(xml_file_name)           
    xroot = xtree.getroot() 

    #Change required from here--> T.XML file
    df_cols = ["title", "movie_id", "parent_id", "rating", "genre", "cast", "director", "writer", "producer", "distributor_name", "running_time", "release_date", "release_notes", "country",  "file_date"]
    #df_cols = ["movie_name", "movie_id", "theater_id", "show_date", "show_times_count","count_value","country_code", "week_date"]
    rows = []

    #To extract data from multiple columns such as genre, cast, writer, producer
    def extract(parameter_name, node):

        if parameter_name == 'release_date':
            if node.find('release_date') is None:
                return '0001-01-01' #'2000-01-01'
            elif node.find('release_date').text is None:
                return '0001-01-01' #'2000-01-01'
            else:
                processing_text = node.find('release_date').text
                numbers = ''
                for each_letter in processing_text:
                    if each_letter.isnumeric():
                        numbers = numbers + each_letter
                
                get_position = processing_text.find(numbers[0])

                month = processing_text[:get_position-1] #if get_position is not -1 else month = ''
                months = {'January':'01', 'February': '02', 'March':'03', 'April':'04', 'May':'05', 'June':'06', 'July':'07', 'August':'08', 'September':'09', 'October':'10', 'November':'11', 'December':'12' }
                l = list(months.keys())
                for m in l:
                    if m == month:
                        format_month = months[m]
                year = numbers[-4:]
                month_1 = format_month
                day = numbers[:-4] 
                #print('y: {}, m:{}, d:{}'.format(type(year), type(month_1), type(day)))
                #print(year +'-'+month_1+'-'+day)
                return year +'-'+month_1+'-'+day

        else:
            x = node.findall(parameter_name)
            if len(x)>=1:
                s = ''
                for i in x:
                    if i.text is None:
                        s = s + ''
                    else:
                        s = s + i.text + '; '
                t_parameter_name = s[:-2]
                #print('Field text: {}'.format(s))
            else:
                t_parameter_name = ''
        
        return t_parameter_name

    def file_date(date_input):
        return date_input[:4]+'-'+date_input[4:6]+'-'+date_input[6:]
        
    def running_time(node):
        if node.find('running_time') is None:
            return 0
        elif node.find('running_time').text is None:
            return 0
        else:
            test = node.find('running_time').text
            c = 0
            for i in test:
                if i.isnumeric():
                    c = 1
                else:
                    continue
            if c == 1:
                return int(node.find('running_time').text)
            else:
                return 0
    for node in xroot:

        # Title, movie_id, parent_id, rating
        t_title = node.find("title").text if node.find("title") is not None else ""
        #print('title: {}'.format(t_title))
        t_movie_id = int(node.find("movie_id").text) if node.find("movie_id") is not None else 0 
        t_parent_id = int(node.find("parent_id").text) if node.find("parent_id") is not None else 0

        t_rating = extract('rating',node)
        t_genre = extract('genre',node)
        t_cast = extract('cast',node)
        t_writer = extract('writer', node)
        t_producer = extract('producer', node)
        t_running_time = running_time(node)
        t_release_date = extract('release_date', node)
        t_release_notes = extract('release_notes', node)
        t_country = country_input  #extract('country', node)                      #change this to intl 
        t_distributor = extract('distributor', node)
        t_director = extract('director', node)

        t_file_date = file_date(date_input)

        #t_country = node.find('intl').attrib['country']
    
        rows.append({"title":t_title, "movie_id":t_movie_id, "parent_id":t_parent_id, "rating":t_rating, "genre":t_genre, "cast":t_cast, "director":t_director, "writer":t_writer, "producer":t_producer, "distributor_name":t_distributor, "running_time":t_running_time, "release_date":t_release_date, "release_notes":t_release_notes, "country":t_country, "file_date":t_file_date})
    out_df = pd.DataFrame(rows, columns = df_cols)
  
    for each_word in df_cols:
        if each_word in ['movie_id', 'parent_id', 'running_time', 'file_date', 'release_date']:
            continue
        out_df[each_word] = out_df[each_word].str.replace(',',';')


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
        csv_file_name = change_in_path + date_input + '_draft_version_csv.csv'
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


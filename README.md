# Kinomatics
Process Application to upload data from multiple XML Files to PostgreSQL

This repositry consists of 3 files.
1. new_build_s.py     --> To upload S.XML files
2. new_build_t.py     --> To upload T.XML files
3. new_build_i.py     --> To upload I.XML files

To work with each of the above files, 3 sections of the code must be changed.

a. Change the Database credentials as required  --> Line 208

   conn_params_dic = {
        "host"      : 'Enter Name of the host',
        "database"  : "Enter Name of the database",
        "user"      : "Enter Database user name",
        "password"  : "Enter Database password"
    }
    
b. Change the path to the location of your XML files  --> Line 15

   change_in_path = '/data/code_testing_2/'
   
c. Change the Postgresql table name according to user requirement --> Line 31
   
   table_name = 'movies'    #Name of the table in PostgreSQL database
   
4. The new_build_s.py uploads the following columns from S.XML to a table in PostgreSQL Database.
Columns:
   Entity         Data Type    Default Value
   movie_name --> Text         ''  (Empty string)
   movie_id --> Integer        0   
   theater_id --> Integer      0
   show_date --> Date          0001-01-01
   show_times_count --> Text   ''
   count_value --> Integer     0
   country_code --> Text       ''
   week_date --> Date          0001-01-01
   
2. new_build_t.py --> T.XML files
Columns:
    Entity        Data Type    Default Value \n
    venue_id --> Integer       0  \n
    name     --> Text          ''  (Empty string)  \n
    screens --> Text           ''  (Empty string) \n
    state --> Text             ''  (Empty string) \n
    country --> Text           ''  (Empty string) \n
    city --> Text              ''  (Empty string) \n
    market --> Text            ''  (Empty string) \n
    closed_reason --> Text     ''  (Empty string) \n
    zip --> Text               ''  (Empty string) \n
    lat --> Text               ''  (Empty string) \n
    long --> Text              ''  (Empty string) \n
    type --> Text              ''  (Empty string) \n
    file_date --> Date         0001-01-01  \n
    
3. new_build_i.py --> I.XML files
Columns:
    Entity        Data Type    Default Value \n
    title --> Text             ''  (Empty string) \n
    movie_id --> Integer       0 \n
    parents_id --> Integer     0 \n
    rating --> Text            ''  (Empty string) \n
    genre --> Text             ''  (Empty string) \n
    cast --> Text              ''  (Empty string) \n
    director --> Text          ''  (Empty string) \n
    writer --> Text            ''  (Empty string) \n
    producer --> Text          ''  (Empty string) \n
    distributor_name --> Text  ''  (Empty string) \n
    running_time --> Integer   0 \n
    release_date --> Date      0001-01-01 \n
    release_notes --> Text     ''  (Empty string) \n
    country --> Text           ''  (Empty string) \n
    file_date --> Date         0001-01-01 \n

Prior to running the above code, a table matching the repective XML file fields(columns) with specified data type should be created in the PostgreSQL Database. If a table already exists, forget this step.

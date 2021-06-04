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
    Entity        Data Type    Default Value 
    venue_id --> Integer       0  
    name     --> Text          ''  (Empty string)  
    screens --> Text           ''  (Empty string) 
    state --> Text             ''  (Empty string) 
    country --> Text           ''  (Empty string) 
    city --> Text              ''  (Empty string) 
    market --> Text            ''  (Empty string) 
    closed_reason --> Text     ''  (Empty string) 
    zip --> Text               ''  (Empty string) 
    lat --> Text               ''  (Empty string) 
    long --> Text              ''  (Empty string) 
    type --> Text              ''  (Empty string) 
    file_date --> Date         0001-01-01  
    
3. new_build_i.py --> I.XML files
Columns:
    Entity        Data Type    Default Value 
    title --> Text             ''  (Empty string) 
    movie_id --> Integer       0 
    parents_id --> Integer     0 
    rating --> Text            ''  (Empty string) 
    genre --> Text             ''  (Empty string) 
    cast --> Text              ''  (Empty string) 
    director --> Text          ''  (Empty string) 
    writer --> Text            ''  (Empty string) 
    producer --> Text          ''  (Empty string) 
    distributor_name --> Text  ''  (Empty string) 
    running_time --> Integer   0 
    release_date --> Date      0001-01-01 
    release_notes --> Text     ''  (Empty string) 
    country --> Text           ''  (Empty string) 
    file_date --> Date         0001-01-01 

Prior to running the above code, a table matching the repective XML file fields(columns) with specified data type should be created in the PostgreSQL Database. If a table already exists, forget this step.

/*---------------------------------STEP 2: Create file format CSV----------------------------*/
create or replace file format aviv.bronze.csv_format
    TYPE = 'CSV' 
    FIELD_DELIMITER = '|' 
    RECORD_DELIMITER = '\n'  //enter
    SKIP_HEADER = 1  //skip header of the CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' //set to double-quote 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

/*---------------------------------STEP 3: Create internal stage----------------------------*/    
create or replace stage aviv.bronze.ingest_stg
  file_format = aviv.bronze.csv_format



--------------------Show file formats
show file formats in schema aviv.bronze;
show stages in schema aviv.bronze;

# define "rideshare_data_data_entry" function
def rideshare_data_data_entry(batch_size: int, env_file_path: str, input_file_path: str) -> dict[str, str]: #type: ignore
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
        from dotenv import dotenv_values
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
        import pandas
        import math
        import psycopg2
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '01', 'message' : str(error)}

    # appending system path and importing "log_writer" function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '02', 'message' : str(error)}

    # define folder path object:S03
    try:
        env_file_path_object = Path(env_file_path)
        rideshare_input_parquet_file_path_object = Path(input_file_path)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '03', 'message' : str(error)}

    # check if ".env" file is present:S04
    try:
        if ((env_file_path_object.exists()) and (env_file_path_object.is_file())):
            env_values = dotenv_values(str(env_file_path_object))
            log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '04', message = 'environment file loaded into script')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '04', 'message' : '".env" File Is Missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '04', 'message' : str(error)}

    # check if input file is present:S05
    try:
        if ((rideshare_input_parquet_file_path_object.exists()) and (rideshare_input_parquet_file_path_object.is_file()) and (rideshare_input_parquet_file_path_object.suffix.lower() == '.parquet')):
            log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '05', message = f'"{rideshare_input_parquet_file_path_object.name}" file is present')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '05', 'message' : f'"{rideshare_input_parquet_file_path_object.name}" file is missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '05', 'message' : str(error)}

    # define "PostgreSQL" database connection parameter:S06
    try:
        database_connection_parameter = {
            'dbname'    : str(env_values.get('DB_NAME')),
            'user'      : str(env_values.get('DB_USERNAME')),
            'password'  : str(env_values.get('DB_PASSWORD')),
            'host'      : str(env_values.get('DB_HOST')),
            'port'      : str(env_values.get('DB_PORT'))
        }
        log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '06', message = 'database connection parameter defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '06', 'message' : str(error)}

    # define "rideshare_data" table present check SQL:S07
    try:
        taxi_zone_lookup_table_present_check_sql = '''
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'rideshare_data'
        );'''
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '07', 'message' : str(error)}

    # define "rideshare_data" insert sql:S08
    try:
        rideshare_data_insert_sql = '''
        INSERT INTO rideshare_data (
            business,
            pickup_location,
            dropoff_location,
            trip_length,
            request_to_dropoff,
            request_to_pickup,
            total_ride_time,
            on_scene_to_pickup,
            on_scene_to_dropoff,
            time_of_day,
            ride_date,
            passenger_fare,
            driver_total_pay,
            rideshare_profit,
            hourly_rate,
            dollars_per_mile,
            row_inserted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());'''
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '08', 'message' : str(error)}

    # check if "rideshare_data" table already present:S09
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_table_present_check_sql)
                if (database_cursor.fetchone()[0]):
                    log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '09', message = '"rideshare_data" table is present inside database')
                else:
                    return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '09', 'message' : '"rideshare_data" table not present inside database'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '09', 'message' : str(error)}

    # open parquet file create scanner object:S10
    try:
        input_parquet_dataset = ds.dataset(str(input_file_path), format = 'parquet')
        input_parquet_reader = input_parquet_dataset.to_batches(batch_size = int(batch_size))
        total_rows = pq.ParquetFile(str(input_file_path)).metadata.num_rows
        log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '10', message = 'input parquet file metadata loaded and reader object created')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '10', 'message' : str(error)}

    # define "clean_int" function
    def clean_int(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return int(value)

    # define "clean_numeric" function
    def clean_numeric(value):
        if value is None:
            return None
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value

    # loop through all the batch
    count = 0
    for batch_record in input_parquet_reader:
        inserted_rows = []
        # load rows into dataframe:S11
        try:
            input_parquet_dataframe = batch_record.to_pandas()
            log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Data-Entry', step = '11', message = f'total "{input_parquet_dataframe.shape[0]}" rows loaded into memory')
        except Exception as error:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '11', 'message' : str(error)}

        # creating custom tuple for insertion:S12
        try:
            for _, row in input_parquet_dataframe.iterrows():
                row_value = (
                    row.get('business'), # business
                    clean_int(row.get('pickup_location')), # pickup_location
                    clean_int(row.get('dropoff_location')), # dropoff_location
                    row.get('trip_length'), # trip_length
                    clean_int(row.get('request_to_dropoff')), # request_to_dropoff
                    clean_int(row.get('request_to_pickup')), # request_to_pickup
                    clean_int(row.get('total_ride_time')), # total_ride_time
                    clean_int(row.get('on_scene_to_pickup')), # on_scene_to_pickup
                    clean_int(row.get('on_scene_to_dropoff')), # on_scene_to_dropoff
                    row.get('time_of_day'), # time_of_day
                    pandas.to_datetime(row.get('date'), errors='coerce').date() if not pandas.isna(row.get('date')) else None, # date
                    row.get('passenger_fare'), # passenger_fare
                    row.get('driver_total_pay'), # driver_total_pay
                    row.get('rideshare_profit'), # rideshare_profit
                    clean_numeric(row.get('hourly_rate')), # hourly_rate
                    clean_numeric(row.get('dollars_per_mile')) # dollars_per_mile
                )
                inserted_rows.append(row_value)
                count += 1
        except Exception as error:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '12', 'message' : str(error)}

        # inserting data into table:S13
        try:
            with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
                with database_connection.cursor() as database_cursor:
                    database_cursor.executemany(rideshare_data_insert_sql, inserted_rows)
                    database_connection.commit()
                    log_writer(status = 'INFO', script_name = 'Rideshare-Data-Data-Entry', step = '13', message = f'total "{len(inserted_rows)}" rows inserted')
                    print(f'INFO - Total: "{count:>10}/{total_rows}" Rows Inserted')
        except Exception as error:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '13', 'message' : str(error)}

        # delete dataframe and list to reduce memory overload
        del inserted_rows
        del input_parquet_dataframe

    return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Data-Entry', 'step' : '12', 'message' : 'all data inserted into "rideshare_data" table'}
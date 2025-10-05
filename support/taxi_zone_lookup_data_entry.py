# define "taxi_zone_lookup_data_entry" function
def taxi_zone_lookup_data_entry() -> dict[str, str]: #type: ignore
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
        from dotenv import dotenv_values
        import pandas
        import psycopg2
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '01', 'message' : str(error)}

    # appending system path and importing "log_writer" function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '02', 'message' : str(error)}

    # define folder and file path:S03
    try:
        parent_folder_path = Path.cwd()
        env_file_path = Path(parent_folder_path) / '.env'
        data_archive_folder_path = Path(parent_folder_path) / 'Data_Archive'
        taxi_zone_lookup_input_csv_file_path = Path(data_archive_folder_path) / 'taxi_zone_lookup.csv'
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '03', 'message' : str(error)}

    # check if ".env" file is present:S04
    try:
        if ((env_file_path.exists()) and (env_file_path.is_file())):
            env_values = dotenv_values(str(env_file_path))
            log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '04', message = 'environment file loaded into script')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '04', 'message' : '".env" File Is Missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '04', 'message' : str(error)}

    # check if input file is present:S05
    try:
        if ((taxi_zone_lookup_input_csv_file_path.exists()) and (taxi_zone_lookup_input_csv_file_path.is_file()) and (taxi_zone_lookup_input_csv_file_path.suffix.lower() == '.csv')):
            log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '05', message = f'"{taxi_zone_lookup_input_csv_file_path.name}" file is present')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '05', 'message' : f'"{taxi_zone_lookup_input_csv_file_path.name}" file is missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '05', 'message' : str(error)}

    # define "PostgreSQL" database connection parameter:S06
    try:
        database_connection_parameter = {
            'dbname'    : str(env_values.get('DB_NAME')),
            'user'      : str(env_values.get('DB_USERNAME')),
            'password'  : str(env_values.get('DB_PASSWORD')),
            'host'      : str(env_values.get('DB_HOST')),
            'port'      : str(env_values.get('DB_PORT'))
        }
        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '06', message = 'database connection parameter defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '06', 'message' : str(error)}

    # define "taxi_zone_lookup" table present check SQL:S07
    try:
        taxi_zone_lookup_table_present_check_sql = '''
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'taxi_zone_lookup'
        );'''
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '07', 'message' : str(error)}

    # check if "taxi_zone_lookup" table already present:S08
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_table_present_check_sql)
                if (database_cursor.fetchone()[0]):
                    log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '08', message = '"taxi_zone_lookup" table is present inside database')
                else:
                    return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '08', 'message' : '"taxi_zone_lookup" table not present inside database'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '08', 'message' : str(error)}

    # load input file:S09
    try:
        input_dataframe = pandas.read_csv(str(taxi_zone_lookup_input_csv_file_path), header = 0)
        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '09', message = f'input file loaded with "{input_dataframe.shape[0]}" rows')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '09', 'message' : str(error)}

    # define row upsert sql:S10
    try:
        taxi_zone_lookup_upsert_sql = '''
        INSERT INTO taxi_zone_lookup (location_id, borough, zone, service_zone)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id)
        DO UPDATE
            SET borough = EXCLUDED.borough,
                zone = EXCLUDED.zone,
                service_zone = EXCLUDED.service_zone
        WHERE taxi_zone_lookup.borough IS DISTINCT FROM EXCLUDED.borough
        OR taxi_zone_lookup.zone IS DISTINCT FROM EXCLUDED.zone
        OR taxi_zone_lookup.service_zone IS DISTINCT FROM EXCLUDED.service_zone;'''
        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '10', message = 'upsert sql define for row insertion')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '10', 'message' : str(error)}

    # creating custom list for data insertion:S11
    try:
        # define empty list
        data_records = []
        count = 0
        # loop through all the rows
        for _, row in input_dataframe.iterrows():
            # check if "LocationID" field not empty
            if ((pandas.isna(row.get('LocationID'))) or (str(row.get('LocationID')).strip() == '')):
                # skip the row insertion
                continue
            # preparing value and append into list
            else:
                row_value = (
                    row.get('LocationID'),
                    row.get('Borough'),
                    row.get('Zone'),
                    row.get('service_zone')
                )
                data_records.append(row_value)
                count += 1
        log_writer(status = 'INFO', script_name = 'Taxi-Zone-Lookup-Data-Entry', step = '11', message = f'total "{int(input_dataframe.shape[0]) - count}" rows skipped to insertion')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '11', 'message' : str(error)}

    # upsert row value into database:S12
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection:  # type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.executemany(taxi_zone_lookup_upsert_sql, data_records)
                database_connection.commit()
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '12', 'message' : str(error)}

    # check all the row inserted:S13
    try:
        taxi_zone_lookup_row_count_sql = '''
        SELECT COUNT(*)
        FROM taxi_zone_lookup'''
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_row_count_sql)
                if (int(database_cursor.fetchone()[0]) == count):
                    del input_dataframe
                    return {'status' : 'SUCCESS', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '13', 'message' : f'total "{count}" rows upserted into "taxi_zone_lookup" table'}
                else:
                    database_cursor.execute('TRUNCATE TABLE taxi_zone_lookup RESTART IDENTITY CASCADE;')
                    return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '13', 'message' : 'data not inserted hence clear out "taxi_zone_lookup" table'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '13', 'message' : str(error)}
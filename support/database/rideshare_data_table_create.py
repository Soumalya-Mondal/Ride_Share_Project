# define "rideshare_data_table_create" function
def rideshare_data_table_create(env_file_path: str) -> dict[str, str]: #type: ignore
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
        from dotenv import dotenv_values
        import psycopg2
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '01', 'message' : str(error)}

    # appending system path and importing "log_writer" function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '02', 'message' : str(error)}

    # define folder path object:S03
    try:
        env_file_path_object = Path(env_file_path)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '03', 'message' : str(error)}

    # check if ".env" file is present:S04
    try:
        if ((env_file_path_object.exists()) and (env_file_path_object.is_file())):
            env_values = dotenv_values(str(env_file_path_object))
            log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Table-Create', step = '04', message = 'environment file loaded into script')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '04', 'message' : '".env" file is missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '04', 'message' : str(error)}

    # define "PostgreSQL" database connection parameter:S05
    try:
        database_connection_parameter = {
            'dbname'    : str(env_values.get('DB_NAME')),
            'user'      : str(env_values.get('DB_USERNAME')),
            'password'  : str(env_values.get('DB_PASSWORD')),
            'host'      : str(env_values.get('DB_HOST')),
            'port'      : str(env_values.get('DB_PORT'))
        }
        log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Table-Create', step = '05', message = 'database connection parameter defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '05', 'message' : str(error)}

    # define "rideshare_data" table SQL:S06
    try:
        rideshare_data_table_sql = '''
        CREATE TABLE rideshare_data (
            id SERIAL PRIMARY KEY,
            business VARCHAR(50),
            pickup_location INT NOT NULL,
            dropoff_location INT NOT NULL,
            trip_length NUMERIC(5,2),
            request_to_dropoff INT,
            request_to_pickup INT,
            total_ride_time INT,
            on_scene_to_pickup INT,
            on_scene_to_dropoff INT,
            time_of_day VARCHAR(20),
            ride_date DATE,
            passenger_fare NUMERIC(10,2),
            driver_total_pay NUMERIC(10,2),
            rideshare_profit NUMERIC(10,2),
            hourly_rate NUMERIC(12,6),
            dollars_per_mile NUMERIC(12,6),
            row_inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            row_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_pickup_location
                FOREIGN KEY (pickup_location)
                REFERENCES taxi_zone_lookup (location_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            CONSTRAINT fk_dropoff_location
                FOREIGN KEY (dropoff_location)
                REFERENCES taxi_zone_lookup (location_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
        );
        ALTER TABLE rideshare_data OWNER TO soumalya;'''
        log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Table-Create', step = '06', message = '"rideshare_data" table create sql defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '06', 'message' : str(error)}

    # define "rideshare_data" table present check SQL:S07
    try:
        rideshare_data_table_present_check_sql = '''
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'rideshare_data'
        );'''
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '07', 'message' : str(error)}

    # check if "rideshare_data" table already present:S08
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(rideshare_data_table_present_check_sql)
                if (database_cursor.fetchone()[0]):
                    # check if table has data
                    database_cursor.execute('SELECT COUNT(*) FROM rideshare_data')
                    # if not data present
                    if (int(database_cursor.fetchone()[0]) == 0):
                        # execute drop table SQL
                        database_cursor.execute('DROP TABLE rideshare_data')
                        log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Table-Create', step = '08', message = '"rideshare_data" table present and empty hence dropped from the database')
                    else:
                        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '08', 'message' : '"rideshare_data" table already present inside database with data'}
                else:
                    log_writer(status = 'SUCCESS', script_name = 'Rideshare-Data-Table-Create', step = '08', message = '"rideshare_data" table not present inside database')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '08', 'message' : str(error)}

    # execute "rideshare_data" table create SQL:S09
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(rideshare_data_table_sql)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '09', 'message' : str(error)}

    # check if "rideshare_data" table created:S10
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(rideshare_data_table_present_check_sql)
                if (not (database_cursor.fetchone()[0])):
                    return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '10', 'message' : '"rideshare_data" table not created inside database'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '10', 'message' : str(error)}

    # execute table trigger function sql:S11
    try:
        rideshare_data_trigger_function = '''
        CREATE OR REPLACE FUNCTION update_row_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.row_updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;'''
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(rideshare_data_trigger_function)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '11', 'message' : str(error)}

    # execute table trigger definition sql:S12
    try:
        rideshare_data_trigger_definition_sql = '''
        CREATE TRIGGER trg_update_row_updated_at
        BEFORE UPDATE ON rideshare_data
        FOR EACH ROW
        EXECUTE FUNCTION update_row_updated_at();'''
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(rideshare_data_trigger_definition_sql)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '12', 'message' : str(error)}

    # check if trigger function created:S13
    try:
        trigger_check_sql = '''
        SELECT COUNT(*)
        FROM information_schema.triggers
        WHERE event_object_table = 'rideshare_data'
        AND trigger_name = 'trg_update_row_updated_at';'''
        with psycopg2.connect(**database_connection_parameter) as database_connection:  # type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(trigger_check_sql)
                if (database_cursor.fetchone()[0] > 0):
                    return {'status' : 'SUCCESS', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '13', 'message' : '"rideshare_data" table created inside database'}
                else:
                    # execute drop table sql
                    database_cursor.execute('DROP TABLE rideshare_data')
                    return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '13', 'message' : 'trigger function not created hence "rideshare_data" table dropped'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Rideshare-Data-Table-Create', 'step' : '13', 'message' : str(error)}
# define "taxi_zone_lookup_table_create" function
def taxi_zone_lookup_table_create(env_file_path: str) -> dict[str, str]: #type: ignore
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
        from dotenv import dotenv_values
        import psycopg2
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '01', 'message' : str(error)}

    # appending system path and importing "log_writer" function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '02', 'message' : str(error)}

    # converting ".env" file into path object:S03
    try:
        env_file_path_object = Path(env_file_path)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '03', 'message' : str(error)}

    # check if ".env" file is present:S04
    try:
        if ((env_file_path_object.exists()) and (env_file_path_object.is_file())):
            env_values = dotenv_values(str(env_file_path_object))
            log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '04', message = 'environment file loaded into script')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '04', 'message' : '".env" file is missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '04', 'message' : str(error)}

    # define "PostgreSQL" database connection parameter:S05
    try:
        database_connection_parameter = {
            'dbname'    : str(env_values.get('DB_NAME')),
            'user'      : str(env_values.get('DB_USERNAME')),
            'password'  : str(env_values.get('DB_PASSWORD')),
            'host'      : str(env_values.get('DB_HOST')),
            'port'      : str(env_values.get('DB_PORT'))
        }
        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '05', message = 'database connection parameter defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '05', 'message' : str(error)}

    # define "taxi_zone_lookup" table SQL:S06
    try:
        taxi_zone_lookup_table_sql = '''
        CREATE TABLE taxi_zone_lookup (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            location_id INT NOT NULL UNIQUE,
            borough VARCHAR(100) NOT NULL,
            zone VARCHAR(200) NOT NULL,
            service_zone VARCHAR(100) NOT NULL,
            row_inserted_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            row_updated_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        ALTER TABLE taxi_zone_lookup OWNER TO soumalya;'''
        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '06', message = '"taxi_zone_lookup" table create sql defined')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '06', 'message' : str(error)}

    # define "taxi_zone_lookup" table present check SQL:S07
    try:
        taxi_zone_lookup_table_present_check_sql = '''
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'taxi_zone_lookup'
        );'''
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '07', 'message' : str(error)}

    # check if "taxi_zone_lookup" table already present:S08
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_table_present_check_sql)
                if (database_cursor.fetchone()[0]):
                    # check if table has data
                    database_cursor.execute('SELECT COUNT(*) FROM taxi_zone_lookup')
                    # if not data present
                    if (int(database_cursor.fetchone()[0]) == 0):
                        # execute drop table SQL
                        database_cursor.execute('DROP TABLE taxi_zone_lookup')
                        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '08', message = '"taxi_zone_lookup" table present and empty hence dropped from the database')
                    else:
                        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '08', 'message' : '"taxi_zone_lookup" table already present inside database with data'}
                else:
                    log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '08', message = '"taxi_zone_lookup" table not present inside database')
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '08', 'message' : str(error)}

    # execute "taxi_zone_lookup" table create SQL:S09
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_table_sql)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '09', 'message' : str(error)}

    # check if "taxi_zone_lookup" table created:S10
    try:
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_table_present_check_sql)
                if (not (database_cursor.fetchone()[0])):
                    return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '10', 'message' : '"taxi_zone_lookup" table not created inside database'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '10', 'message' : str(error)}

    # execute table trigger function sql:S11
    try:
        taxi_zone_lookup_trigger_function_sql = '''
        CREATE OR REPLACE FUNCTION update_row_updated_time()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.row_updated_time = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;'''
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_trigger_function_sql)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '11', 'message' : str(error)}

    # execute table trigger definition sql:S12
    try:
        taxi_zone_lookup_trigger_definition_sql = '''
        CREATE TRIGGER trg_update_row_updated_time
        BEFORE UPDATE ON taxi_zone_lookup
        FOR EACH ROW
        EXECUTE FUNCTION update_row_updated_time();'''
        with psycopg2.connect(**database_connection_parameter) as database_connection: #type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(taxi_zone_lookup_trigger_definition_sql)
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '12', 'message' : str(error)}

    # check if trigger function created:S13
    try:
        trigger_check_sql = '''
        SELECT COUNT(*)
        FROM information_schema.triggers
        WHERE event_object_table = 'taxi_zone_lookup'
        AND trigger_name = 'trg_update_row_updated_time';'''
        with psycopg2.connect(**database_connection_parameter) as database_connection:  # type: ignore
            with database_connection.cursor() as database_cursor:
                database_cursor.execute(trigger_check_sql)
                if (database_cursor.fetchone()[0] > 0):
                    return {'status' : 'SUCCESS', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '13', 'message' : '"taxi_zone_lookup" table created inside database'}
                else:
                    # execute drop table sql
                    database_cursor.execute('DROP TABLE taxi_zone_lookup')
                    return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '13', 'message' : 'trigger function not created hence "taxi_zone_lookup" table dropped'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '13', 'message' : str(error)}
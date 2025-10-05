# define "taxi_zone_lookup_table_create" function
def taxi_zone_lookup_table_create() -> dict[str, str]: #type: ignore
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

    # define folder and file path:S03
    try:
        parent_folder_path = Path.cwd()
        env_file_path = Path(parent_folder_path) / '.env'
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '03', 'message' : str(error)}

    # check if ".env" file is present:S04
    try:
        if (env_file_path.exists() and env_file_path.is_file()):
            env_values = dotenv_values(str(env_file_path))
            log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '04', message = 'envrionment file loaded into script')
        else:
            return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '04', 'message' : '".env" File Is Missing'}
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
            service_zone VARCHAR(100) NOT NULL
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
        )'''
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
                        log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '08', message = '"taxi_zone_lookup" table is empty and dropped from the database')
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
                if (database_cursor.fetchone()[0]):
                    log_writer(status = 'SUCCESS', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '10', message = '"taxi_zone_lookup" table created inside database')
                    return {'status' : 'SUCCESS', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '10', 'message' : '"taxi_zone_lookup" table created inside database'}
                else:
                    log_writer(status = 'ERROR', script_name = 'Taxi-Zone-Lookup-Table-Create', step = '10', message = '"taxi_zone_lookup" table not created inside database')
                    return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '10', 'message' : '"taxi_zone_lookup" table not created inside database'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Table-Create', 'step' : '10', 'message' : str(error)}
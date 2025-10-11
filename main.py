# define main function
if __name__ == '__main__':
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
    except Exception as error:
        print(f'ERROR - [Main:S01] - {str(error)}')

    # appending system path and importing user-define function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
        from support.database.taxi_zone_lookup_table_create import taxi_zone_lookup_table_create
        from support.database.taxi_zone_lookup_data_entry import taxi_zone_lookup_data_entry
        from support.database.rideshare_data_table_create import rideshare_data_table_create
        from support.database.rideshare_data_data_entry import rideshare_data_data_entry
    except Exception as error:
        print(f'ERROR - [Main:S02] - {str(error)}')

    # define folder and file path:S03
    try:
        parent_folder_path = Path.cwd()
        data_archive_folder_path = Path(parent_folder_path) / 'Data_Archive'
        env_file_path = Path(parent_folder_path) / '.env'
        taxi_zone_lookup_input_file_path = Path(data_archive_folder_path) / 'taxi_zone_lookup.csv'
        rideshare_data_input_file_path = Path(data_archive_folder_path) / 'rideshare_data.parquet'
        log_writer(status = 'SUCCESS', script_name = 'Main', step = '03', message = 'all folder and files path defined')
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '03', message = str(error))
        print(f'ERROR - [Main:S03] - {str(error)}')

    # checking if all files are present:S04
    try:
        # checking "env" file
        if (not (env_file_path.exists() and env_file_path.is_file())):
            log_writer(status = 'ERROR', script_name = 'Main', step = '04', message = '".env" file is missing')
            print('ERROR - ".env" File Is Missing')
            exit(1)
        # checking "taxi_zone_lookup.csv" file
        if (not (taxi_zone_lookup_input_file_path.exists() and taxi_zone_lookup_input_file_path.is_file())):
            log_writer(status = 'ERROR', script_name = 'Main', step = '04', message = '"taxi_zone_lookup.csv" file is missing')
            print('ERROR - "taxi_zone_lookup.csv" File Is Missing')
            exit(1)
        # checking "rideshare_data.parquet" file
        if (not (rideshare_data_input_file_path.exists() and rideshare_data_input_file_path.is_file())):
            log_writer(status = 'ERROR', script_name = 'Main', step = '04', message = '"rideshare_data.parquet" file is missing')
            print('ERROR - "rideshare_data.parquet" File Is Missing')
            exit(1)
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '04', message = str(error))
        print(f'ERROR - [Main:S04] - {str(error)}')

    # executing "taxi_zone_lookup_table_create" function to create "taxi_zone_lookup" table:S05
    try:
        taxi_zone_lookup_table_create_backend_response = taxi_zone_lookup_table_create(env_file_path = str(env_file_path))
        # check result
        if (str(taxi_zone_lookup_table_create_backend_response['status']).lower() == 'error'):
            print(f"ERROR - [{taxi_zone_lookup_table_create_backend_response['script_name']}] - [{taxi_zone_lookup_table_create_backend_response['step']}] - {taxi_zone_lookup_table_create_backend_response['message']}")
            exit(1)
        if (str(taxi_zone_lookup_table_create_backend_response['status']).lower() == 'success'):
            log_writer(status = 'SUCCESS', script_name = 'Main', step = '05', message = str(taxi_zone_lookup_table_create_backend_response['message']))
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '05', message = str(error))
        print(f'ERROR - [Main:S05] - {str(error)}')

    # executing "taxi_zone_lookup_data_entry" function to insert data into database:S06
    try:
        taxi_zone_lookup_data_entry_backend_response = taxi_zone_lookup_data_entry(env_file_path = str(env_file_path), input_file_path = str(taxi_zone_lookup_input_file_path))
        # check result
        if (str(taxi_zone_lookup_data_entry_backend_response['status']).lower() == 'error'):
            print(f"ERROR - [{taxi_zone_lookup_data_entry_backend_response['script_name']}] - [{taxi_zone_lookup_data_entry_backend_response['step']}] - {taxi_zone_lookup_data_entry_backend_response['message']}")
            exit(1)
        if (str(taxi_zone_lookup_data_entry_backend_response['status']).lower() == 'success'):
            log_writer(status = 'SUCCESS', script_name = 'Main', step = '06', message = str(taxi_zone_lookup_data_entry_backend_response['message']))
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '06', message = str(error))
        print(f'ERROR - [Main:S06] - {str(error)}')

    # executing "rideshare_data_table_create" function to create "rideshare_data" table:S07
    try:
        rideshare_data_table_create_backend_response = rideshare_data_table_create(env_file_path = str(env_file_path))
        # check result
        if (str(rideshare_data_table_create_backend_response['status']).lower() == 'error'):
            print(f"ERROR - [{rideshare_data_table_create_backend_response['script_name']}] - [{rideshare_data_table_create_backend_response['step']}] - {rideshare_data_table_create_backend_response['message']}")
            exit(1)
        if (str(rideshare_data_table_create_backend_response['status']).lower() == 'success'):
            log_writer(status = 'SUCCESS', script_name = 'Main', step = '07', message = str(rideshare_data_table_create_backend_response['message']))
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '07', message = str(error))
        print(f'ERROR - [Main:S07] - {str(error)}')

    # executing "rideshare_data_data_entry" function to insert data into table:S08
    try:
        rideshare_data_data_entry_backend_response = rideshare_data_data_entry(batch_size = 1000, env_file_path = str(env_file_path), input_file_path = str(rideshare_data_input_file_path))
        # check result
        if (str(rideshare_data_data_entry_backend_response['status']).lower() == 'error'):
            print(f"ERROR - [{rideshare_data_data_entry_backend_response['script_name']}] - [{rideshare_data_data_entry_backend_response['step']}] - {rideshare_data_data_entry_backend_response['message']}")
            exit(1)
        if (str(rideshare_data_data_entry_backend_response['status']).lower() == 'success'):
            log_writer(status = 'SUCCESS', script_name = 'Main', step = '08', message = str(rideshare_data_data_entry_backend_response['message']))
    except Exception as error:
        log_writer(status = 'ERROR', script_name = 'Main', step = '08', message = str(error))
        print(f'ERROR - [Main:S08] - {str(error)}')
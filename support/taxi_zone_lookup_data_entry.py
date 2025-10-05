# define "taxi_zone_lookup_data_entry" function
def taxi_zone_lookup_data_entry() -> dict[str, str]: #type: ignore
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
        from dotenv import dotenv_values
        import cudf
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
            return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '04', 'message' : f'"{taxi_zone_lookup_input_csv_file_path.name}" file is missing'}
    except Exception as error:
        return {'status' : 'ERROR', 'script_name' : 'Taxi-Zone-Lookup-Data-Entry', 'step' : '05', 'message' : str(error)}
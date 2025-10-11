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



# 📘 File Metadata Summary
# ==================================================
# Number of Row Groups: 6
# Number of Columns: 19
# Total Rows: 365,083,103
# Created By: parquet-cpp-arrow version 6.0.1
# Format Version: 1.0

# 📂 Row Group Details
# ==================================================
# Row Group 0:
#   - Num Rows: 67,108,864
#   - Total Byte Size: 2098.36 MB
#   - Columns: 19

# Row Group 1:
#   - Num Rows: 67,108,864
#   - Total Byte Size: 2163.92 MB
#   - Columns: 19

# Row Group 2:
#   - Num Rows: 67,108,864
#   - Total Byte Size: 2212.56 MB
#   - Columns: 19

# Row Group 3:
#   - Num Rows: 67,108,864
#   - Total Byte Size: 2118.92 MB
#   - Columns: 19

# Row Group 4:
#   - Num Rows: 67,108,864
#   - Total Byte Size: 2173.58 MB
#   - Columns: 19

# Row Group 5:
#   - Num Rows: 29,538,783
#   - Total Byte Size: 923.49 MB
#   - Columns: 19

# 🧱 Schema
# ==================================================
# <pyarrow._parquet.ParquetSchema object at 0x74ef077930c0>
# required group field_id=-1 schema {
#   optional binary field_id=-1 business (String);
#   optional int64 field_id=-1 pickup_location;
#   optional int64 field_id=-1 dropoff_location;
#   optional double field_id=-1 trip_length;
#   optional double field_id=-1 request_to_dropoff;
#   optional double field_id=-1 request_to_pickup;
#   optional double field_id=-1 total_ride_time;
#   optional double field_id=-1 on_scene_to_pickup;
#   optional double field_id=-1 on_scene_to_dropoff;
#   optional binary field_id=-1 time_of_day (String);
#   optional int32 field_id=-1 date (Date);
#   optional int64 field_id=-1 hour_of_day;
#   optional int64 field_id=-1 week_of_year;
#   optional int64 field_id=-1 month_of_year;
#   optional double field_id=-1 passenger_fare;
#   optional double field_id=-1 driver_total_pay;
#   optional double field_id=-1 rideshare_profit;
#   optional double field_id=-1 hourly_rate;
#   optional double field_id=-1 dollars_per_mile;
# }
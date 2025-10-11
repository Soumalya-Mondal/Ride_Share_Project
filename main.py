# define main function
if __name__ == '__main__':
    # importing python module:S01
    try:
        from pathlib import Path
        import sys
    except Exception as error:
        print(f'ERROR - [Main:S01] - {str(error)}')

    # appending system path and importing "log_writer" function:S02
    try:
        sys.path.append(str(Path.cwd()))
        from support.log_writer import log_writer
    except Exception as error:
        print(f'ERROR - [Main:S02] - {str(error)}')

    # define folder path:S03
    try:
        parent_folder_path = Path.cwd()
        data_archive_folder_path = Path(parent_folder_path) / 'Data_Archive'
    except Exception as error:
        print(f'ERROR - [Main:S03] - {str(error)}')

    from support.database.taxi_zone_lookup_table_create import taxi_zone_lookup_table_create
    from support.database.taxi_zone_lookup_data_entry import taxi_zone_lookup_data_entry
    print(taxi_zone_lookup_table_create())
    print(taxi_zone_lookup_data_entry())


    from support.database.rideshare_data_table_create import rideshare_data_table_create
    print(rideshare_data_table_create())



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
# define "log_writer" function
def log_writer(status: str, script_name: str, step: str, message: str):
    # importing python module:S01
    try:
        from pathlib import Path
        from datetime import datetime
    except Exception as error:
        print(f'ERROR - [Log-Writer:S01] - {str(error)}')

    # define folder and file path:S02
    try:
        parent_folder_path = Path.cwd()
        activity_log_file_path = Path(parent_folder_path) / 'activity.log'
    except Exception as error:
        print(f'ERROR - [Log-Writer:S02] - {str(error)}')

    # write into file:S03
    try:
        with open(str(activity_log_file_path), mode = 'a', encoding = 'utf-8') as log_file:
            log_file.write(f"{datetime.now().strftime('%d-%b-%Y')} - {datetime.now().strftime('%H:%M:%S')} - {status.upper()} - {script_name.upper()} - {step.upper()} - {message.lower()}\n")
    except Exception as error:
        print(f'ERROR - [Log-Writer:S03] - {str(error)}')
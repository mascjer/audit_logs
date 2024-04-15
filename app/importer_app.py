from datetime import datetime
import pandas as pd
import dash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

#internal imports
from database_functions import *

app = dash.Dash(__name__)

sched = BackgroundScheduler()


def update_audit_log():
    pd.set_option('display.max_columns', None)

    print('Starting Audit Log')
    started = datetime.now()
    
    ########### RATE DATA IMPORT ###########
    started_rate_import = datetime.now()
    print('Collecting new rate data')
    new_rate_data = import_new_data('Rate')
    existing_rate_data = import_existing_data('Rate')
    finished_rate_import = datetime.now()
    print('Rate data collected')
    print('It took', str(finished_rate_import-started_rate_import), ' time to collect new rate data')
    
    
    ########### RATE DATA INSERT ###########
    started_rate_insert = datetime.now()
    print('Inserting audited rate data into audit log')
    check_and_insert_rate_data(new_rate_data, existing_rate_data)
    finished_rate_insert = datetime.now()
    print('Audited rate data inserted into audit log')
    print('It took', str(finished_rate_insert-started_rate_insert), ' time to insert audited rate data')
    
    ########### RATE DATA DELETED ###########
    started_rate_delete = datetime.now()
    print('Inserting deleted audited rate data into audit log')
    check_and_mark_deleted_rate_data(new_rate_data, existing_rate_data)
    finished_rate_delete = datetime.now()
    print('Audited rate data inserted into audit log')
    print('It took', str(finished_rate_delete-started_rate_delete), ' time to insert audited rate data')

    
    del new_rate_data
    del existing_rate_data

    ########### COST DATA IMPORT ###########
    started_cost_import = datetime.now()
    print('Collecting new cost data')
    new_cost_data = import_new_data('Cost')
    existing_cost_data = import_existing_data('Cost')
    finished_cost_import = datetime.now()
    print('Cost data collected')
    print('It took', str(finished_cost_import-started_cost_import), ' time to collect new cost data')

    ########### COST DATA INSERT ###########
    started_cost_insert = datetime.now()
    print('Inserting audited cost data into audit log')
    check_and_insert_cost_data(new_cost_data, existing_cost_data)
    finished_cost_insert = datetime.now()
    print('Audited cost data inserted into audit log')
    print('It took', str(finished_cost_insert-started_cost_insert), ' time to insert audited cost data')

    ########### COST DATA DELETED ###########
    started_cost_delete = datetime.now()
    print('Inserting deleted audited cost data into audit log')
    check_and_mark_deleted_cost_data(new_cost_data, import_existing_data('Cost'))
    finished_cost_delete = datetime.now()
    print('Audited cost data inserted into audit log')
    print('It took', str(finished_cost_delete-started_cost_delete), ' time to insert audited cost data')

    del new_cost_data
    del existing_cost_data
    
    
    finished = datetime.now()
    print('Finished Audit Log')
    print('It took', str(finished-started), ' time to complete')

    return 'finished'


# Define the CronTrigger to run every half hour
trigger = CronTrigger(hour='0-23', minute='0,30')

# Add the job with the CronTrigger
sched.add_job(update_audit_log, id='audit_log', trigger=trigger, replace_existing=True)

sched.start()

if __name__ == "__main__":
     app.run_server(debug=False, host='0.0.0.0', port=3002)

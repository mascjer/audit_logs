import numpy as np
import pandas as pd
import pytz
import csv
import time
import snowflake.connector
import traceback
from datetime import datetime, timezone, timedelta
import warnings
from urllib3.exceptions import InsecureRequestWarning

#Internal Libraries
from connections import get_snowflake_connection
from query import new_rate_query_string, old_rate_query_string, new_cost_query_string, old_cost_query_string

# Filter out the InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def round_total_if_not_null(value):
    if pd.notnull(value):
        return round(value, 2)
    return value


def escape_single_quotes(input_data):
    if isinstance(input_data, str):
        return input_data.replace("'", "''")
    else:
        return input_data


def import_new_data(cost_rate):
    # Get a connection to Snowflake using the get_snowflake_connection() function.
    con = get_snowflake_connection()

    # Execute the query
    cursor = con.cursor()
    if cost_rate == 'Rate':
        cursor.execute(new_rate_query_string)
    elif cost_rate == 'Cost':
        cursor.execute(new_cost_query_string)
    else:
        print('Cost/Rate Not Selected')

    # Fetch the results
    columns = [col[0] for col in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Close the connection
    cursor.close()
    con.close()

    # Convert the AUDITDATETIME to the American Central time zone (America/Chicago)
    if "AUDIT_DATETIME" in columns:
        for result in results:
            if "AUDIT_DATETIME" in result:
                audit_datetime = result["AUDIT_DATETIME"]
                if audit_datetime.tzinfo:
                    result["AUDIT_DATETIME"] = audit_datetime.astimezone(pytz.timezone("America/Chicago")).replace(tzinfo=None)

    # Convert the results to a pandas DataFrame
    df = pd.DataFrame(results)

    # Apply the round_total_if_not_null function to the TOTAL column
    df['TOTAL'] = df['TOTAL'].apply(lambda x: round_total_if_not_null(x))
    df['UPDATE_TYPE'] = 'INSERT'

    return df   


def import_existing_data(cost_rate, batch_size=5000):
    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        print('Getting old data...')

        if cost_rate == 'Rate':
            cursor.execute(old_rate_query_string)
        elif cost_rate == 'Cost':
            cursor.execute(old_cost_query_string)
        else:
            print('Cost/Rate Not Selected')

        print('Old data executed')

        # Fetch data in batches
        all_results = []  # To store all processed data
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
                
            columns = [col[0] for col in cursor.description]
            batch_results = [dict(zip(columns, row)) for row in batch]
            
            # Append the processed batch_results to the result list
            all_results.extend(batch_results)

        # Close the connection
        cursor.close()
        con.close()

        print('Data retrieval completed.')

        # Convert the results to a pandas DataFrame
        df = pd.DataFrame(all_results)

        # Apply the round_total_if_not_null function to the TOTAL column
        df['TOTAL'] = df['TOTAL'].apply(lambda x: round_total_if_not_null(x))

        return df  # Return the collected and processed data as a pandas DataFrame

    except Exception as e:
        print(f"Error: {e}")
    

def get_data_in_snowflake_and_not(query_results, new_data):
    data_in_snowflake = {}
    data_not_in_snowflake = {}

    # Extract existing load_numbers from query_results
    existing_order_numbers = {row['ORDER_NUMBER'] for row in query_results}

    # Compare existing load_numbers with new_data
    for new_row in new_data:
        order_number = new_row['ORDER_NUMBER']
        if order_number in existing_order_numbers:
            data_in_snowflake[order_number] = new_row
        else:
            data_not_in_snowflake[order_number] = new_row

    return data_in_snowflake, data_not_in_snowflake


def upload_new_data_to_snowflake(new_data, cost_rate, batch_size=5000):
    updated_rows = 0
    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        new_data_df = pd.DataFrame(new_data)
        new_data_df['UPDATE_TYPE'] = 'INSERT'

        #columns for insert
        columns_for_insert = [col for col in new_data_df.columns]
        
        # Prepare the values for insertion (handling NULL and quotes for VARCHARs)
        values_list = []
        for _, row in new_data_df.iterrows():
            row_values = []
            for value in row.values:
                if pd.isnull(value):
                    row_values.append(None)
                elif isinstance(value, pd.Timestamp):
                    # Convert Timestamp to string representation
                    row_values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    row_values.append(value)
            values_list.append(tuple(row_values))

        # Construct the insert query with placeholders for VALUES
        placeholders = ', '.join(['%s'] * 10) # for the 10 columns in SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG AND LTL_ORDER_COST_AUDIT_LOG
        columns = ', '.join(columns_for_insert)
        if cost_rate == 'RATE':
            insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG ({}) VALUES ({});".format(
                columns, placeholders
            )
        else:
            insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_COST_LOG ({}) VALUES ({});".format(
                columns, placeholders
            )

        print(new_data_df)

        '''
        # Execute the insert query with the batch of values
        batch_start = 0
        while batch_start < len(values_list):
            batch_end = min(batch_start + batch_size, len(values_list))
            batch_values = values_list[batch_start:batch_end]
            # Ensure all rows in batch_values have the same number of columns as placeholders
            batch_values = [row + tuple([None] * (len(columns_for_insert) - len(row))) for row in batch_values]
            cursor.executemany(insert_query, batch_values)
            updated_rows += len(batch_values)
            batch_start += batch_size

        # Commit the changes and close the connection
        con.commit()
        cursor.close()
        con.close()
        '''
        
        print("Data inserted successfully. " + str(updated_rows) + " were updated.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Snowflake ProgrammingError: {e}")
        print(traceback.format_exc())
        con.rollback()
        con.close()
    except snowflake.connector.errors.DatabaseError as e:
        print(f"Snowflake DatabaseError: {e}")
        print(traceback.format_exc())
        con.rollback()
        con.close()
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
        con.rollback()
        con.close()


def merge_insert_data(new_data, existing_data, cost_rate):
    # Drop the AUDITDATETIME column from data_in_snowflake_df
    existing_data.drop(columns=['AUDIT_DATETIME'], inplace=True)

    # Merge data_in_snowflake_df and existing_data_df based on specific columns
    if cost_rate == 'Rate':
        merge_columns = ['ORDER_NUMBER', 'BILL_TO_CUSTOMER_CODE', 'RATE_CODE', 'RATE_TYPE', 'RATE_SOURCE', 'TOTAL']
    else:
        merge_columns = ['ORDER_NUMBER', 'CARRIER_CODE', 'RATE_CODE', 'RATE_TYPE', 'RATE_SOURCE', 'TOTAL']

    # Merge data_in_snowflake_df and existing_data_df based on LOAD_NUMBER
    merged_df = new_data.merge(existing_data[merge_columns], on=merge_columns, how='left', indicator=True)
    print('merged data')

    # Filter out the matching rows (where the row exists in existing_data_df)
    non_matching_rows = merged_df[merged_df['_merge'] == 'left_only']
    print('filtered data')

    # Drop the '_merge' column as it was used for filtering only
    non_matching_rows.drop(columns=['_merge'], inplace=True)

    return non_matching_rows


def check_and_insert_rate_data(new_rate_data, existing_rate_data, batch_size=10000):
    updated_rows = 0

    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        non_matching_rows = merge_insert_data(new_rate_data, existing_rate_data, 'Rate')

        print('rows to insert:', len(non_matching_rows))

        #columns for insert
        columns_for_insert = [col for col in new_rate_data.columns]
        
        # Prepare the values for insertion (handling NULL and quotes for VARCHARs)
        values_list = []
        for _, row in non_matching_rows.iterrows():
            row_values = []
            for value in row.values:
                if pd.isnull(value):
                    row_values.append(None)
                elif isinstance(value, pd.Timestamp):
                    # Convert Timestamp to string representation
                    row_values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    row_values.append(value)
            values_list.append(tuple(row_values))

        # Construct the insert query with placeholders for VALUES
        placeholders = ', '.join(['%s'] * 10) # for the 10 columns in SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG
        columns = ', '.join(columns_for_insert)
        insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG ({}) VALUES ({});".format(
            columns, placeholders
        )
        
        print('inserting data')

        # Execute the insert query with the batch of values
        batch_start = 0
        while batch_start < len(values_list):
            batch_end = min(batch_start + batch_size, len(values_list))
            batch_values = values_list[batch_start:batch_end]
            # Ensure all rows in batch_values have the same number of columns as placeholders
            batch_values = [row + tuple([None] * (len(columns_for_insert) - len(row))) for row in batch_values]
            cursor.executemany(insert_query, batch_values)
            updated_rows += len(batch_values)
            batch_start += batch_size

        # Commit the changes and close the connection
        con.commit()
        cursor.close()
        con.close()
        
        print("Audited data inserted successfully. " + str(updated_rows) + " were updated.")
    except Exception as e:
        print(f"Error: {e}")
        print(str(updated_rows) + " were updated.")
        con.rollback()
        con.close()


def check_and_insert_cost_data(new_cost_data, existing_cost_data, batch_size=10000):
    updated_rows = 0

    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        non_matching_rows = merge_insert_data(new_cost_data, existing_cost_data, 'Cost')

        print('rows to insert:', len(non_matching_rows))

        #columns for insert
        columns_for_insert = [col for col in new_cost_data.columns]
        
        # Prepare the values for insertion (handling NULL and quotes for VARCHARs)
        values_list = []
        for _, row in non_matching_rows.iterrows():
            row_values = []
            for value in row.values:
                if pd.isnull(value):
                    row_values.append(None)
                elif isinstance(value, pd.Timestamp):
                    # Convert Timestamp to string representation
                    row_values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    row_values.append(value)
            values_list.append(tuple(row_values))

        # Construct the insert query with placeholders for VALUES
        placeholders = ', '.join(['%s'] * 10) # for the 9 columns in SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG
        columns = ', '.join(columns_for_insert)
        insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_COST_AUDIT_LOG ({}) VALUES ({});".format(
            columns, placeholders
        )

        print('inserting data')
        
        # Execute the insert query with the batch of values
        batch_start = 0
        while batch_start < len(values_list):
            batch_end = min(batch_start + batch_size, len(values_list))
            batch_values = values_list[batch_start:batch_end]
            # Ensure all rows in batch_values have the same number of columns as placeholders
            batch_values = [row + tuple([None] * (len(columns_for_insert) - len(row))) for row in batch_values]
            cursor.executemany(insert_query, batch_values)
            updated_rows += len(batch_values)
            batch_start += batch_size

        # Commit the changes and close the connection
        con.commit()
        cursor.close()
        con.close()
        
        print("Audited data inserted successfully. " + str(updated_rows) + " were updated.")
    except Exception as e:
        print(f"Error: {e}")
        print(str(updated_rows) + " were updated.")
        con.rollback()
        con.close()


def merge_deleted_data(new_data, existing_data, cost_rate):
    # Filter out rows where UPDATE_TYPE is not 'DELETED'
    existing_deleted_data_df = existing_data.loc[existing_data['UPDATE_TYPE'] == 'DELETED']

    # Filter out rows where UPDATE_TYPE is not 'DELETED'
    existing_data_df = existing_data.loc[existing_data['UPDATE_TYPE'] != 'DELETED']

    # Drop the AUDITDATETIME column from data_in_snowflake_df
    #existing_data_df.drop(columns=['AUDIT_DATETIME'], inplace=True)

    # Merge data_in_snowflake_df and existing_data_df based on specific columns
    if cost_rate == 'Rate':
        merge_columns = ['ORDER_NUMBER', 'BILL_TO_CUSTOMER_CODE', 'RATE_CODE', 'RATE_TYPE', 'RATE_SOURCE']
    else:
        merge_columns = ['ORDER_NUMBER', 'CARRIER_CODE', 'RATE_CODE', 'RATE_TYPE', 'RATE_SOURCE']
    merged_df = existing_data_df.merge(new_data[merge_columns], on=merge_columns, how='left', indicator=True)

    # Filter out the non-matching rows (where the row exists in existing_data_df)
    non_matching_rows = merged_df[merged_df['_merge'] == 'left_only']

    # Drop the '_merge' column as it was used for filtering only
    non_matching_rows.drop(columns=['_merge'], inplace=True)

    # Filter out rows from non_matching_rows that already exist in existing_deleted_data_df
    non_matching_rows = non_matching_rows[
        ~non_matching_rows.set_index(merge_columns + ['TOTAL']).index
        .isin(existing_deleted_data_df.set_index(merge_columns + ['TOTAL']).index)
    ]

    return non_matching_rows


def check_and_mark_deleted_rate_data(new_delete_rate_data, existing_delete_rate_data, batch_size=10000):
    updated_rows = 0

    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        non_matching_rows = merge_deleted_data(new_delete_rate_data, existing_delete_rate_data, 'Rate')

        print('rows to insert:', len(non_matching_rows))

        if not non_matching_rows.empty:
            central_time = timezone(timedelta(hours=-5))
            current_time = datetime.now(central_time).strftime('%Y-%m-%d %H:%M:%S')
            non_matching_rows['AUDIT_DATETIME'] = current_time
            non_matching_rows['UPDATE_TYPE'] = 'DELETED'

            columns_for_update = non_matching_rows.columns.tolist()

            values_list = []
            for _, row in non_matching_rows.iterrows():
                row_values = []
                for value in row.values:
                    if pd.isnull(value):
                        row_values.append(None)
                    elif isinstance(value, pd.Timestamp):
                        row_values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        row_values.append(value)
                values_list.append(tuple(row_values))

            placeholders = ', '.join(['%s'] * len(columns_for_update))
            columns = ', '.join(columns_for_update)
            insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_RATE_AUDIT_LOG ({}) VALUES ({});".format(
                columns, placeholders
            )

            batch_start = 0
            while batch_start < len(values_list):
                batch_end = min(batch_start + batch_size, len(values_list))
                batch_values = values_list[batch_start:batch_end]
                cursor.executemany(insert_query, batch_values)
                updated_rows += len(batch_values)
                batch_start += batch_size

            con.commit()
            cursor.close()
            con.close()

            print("Audited data marked as 'DELETED' successfully. " + str(updated_rows) + " were updated.")
        else:
            print("No new data to insert.")

    except Exception as e:
        print(f"Error: {e}")
        print(str(updated_rows) + " were updated.")
        con.rollback()
        con.close()


def check_and_mark_deleted_cost_data(new_delete_cost_data, existing_delete_cost_data, batch_size=10000):
    updated_rows = 0

    try:
        con = get_snowflake_connection()
        cursor = con.cursor()

        non_matching_rows = merge_deleted_data(new_delete_cost_data, existing_delete_cost_data, 'Cost')

        print('rows to insert:', len(non_matching_rows))

        if not non_matching_rows.empty:
            central_time = timezone(timedelta(hours=-5))
            current_time = datetime.now(central_time).strftime('%Y-%m-%d %H:%M:%S')
            non_matching_rows['AUDIT_DATETIME'] = current_time
            non_matching_rows['UPDATE_TYPE'] = 'DELETED'

            columns_for_update = non_matching_rows.columns.tolist()

            values_list = []
            for _, row in non_matching_rows.iterrows():
                row_values = []
                for value in row.values:
                    if pd.isnull(value):
                        row_values.append(None)
                    elif isinstance(value, pd.Timestamp):
                        row_values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        row_values.append(value)
                values_list.append(tuple(row_values))

            placeholders = ', '.join(['%s'] * len(columns_for_update))
            columns = ', '.join(columns_for_update)
            insert_query = "INSERT INTO SANDBOX_NAST_LTL_DOMAIN.BASE.LTL_ORDER_COST_AUDIT_LOG ({}) VALUES ({});".format(
                columns, placeholders
            )

            batch_start = 0
            while batch_start < len(values_list):
                batch_end = min(batch_start + batch_size, len(values_list))
                batch_values = values_list[batch_start:batch_end]
                cursor.executemany(insert_query, batch_values)
                updated_rows += len(batch_values)
                batch_start += batch_size

            con.commit()
            cursor.close()
            con.close()

            print("Audited data marked as 'DELETED' successfully. " + str(updated_rows) + " were updated.")
        else:
            print("No new data to insert.")

    except Exception as e:
        print(f"Error: {e}")
        print(str(updated_rows) + " were updated.")
        con.rollback()
        con.close()
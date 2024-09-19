CREATE SECURE FUNCTION IF NOT EXISTS ESTIMATE_REPLICATION_COST(
    database_ids ARRAY, 
    replication_frequency_minutes INT,
    start_time TIMESTAMP_NTZ = NULL,  
    end_time TIMESTAMP_NTZ = NULL
    )
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'estimate_replication_cost'

AS $$
import snowflake.snowpark.functions as F
from datetime import datetime, timedelta
import json

def estimate_replication_cost(database_ids,replication_frequency_minutes, start_time, end_time):
    """
    Estimate replication cost of the given database IDs given the refresh frequency.

    Args:
        database_ids (list of int): List of database IDs to consider
        replication_frequency_minutes (int): Refresh frequency stated in minutes. Maximum value can be 8 days or 8*1440 minutes
        start_time (timestamp): Optional start time parameter. 
        end_time (timestamp): Optional end time parameter.

        if start_time and end_time are set, we only use the data between [start_time, end_time] for our estimate. If this is not provided, we default to using last 2 months data.

    Returns:
        database_ids (list of int): List of database IDs to consider
        replication_frequency_minutes (int): Refresh frequency stated in minutes. Maximum value can be 8 days or 8*1440 minutes
        start_time (timestamp): Optional start time parameter. 
        end_time (timestamp): Optional end time parameter.
    """
    def validate_params(database_ids, replication_frequency_minutes, start_time, end_time):
        """
        Check all the params and immediately return with error if any input is not as expected.

        Also, modify the params if needed

        Args:
            database_ids (list of int): List of database IDs to consider
            replication_frequency_minutes (int): Refresh frequency stated in minutes. Maximum value can be 8 days or 8*1440 minutes
            start_time (timestamp): Optional start time parameter. 
            end_time (timestamp): Optional end time parameter.

        Returns:
            database_ids (list of int): List of database IDs to consider
            replication_frequency_minutes (int): Refresh frequency stated in minutes. Maximum value can be 8 days or 8*1440 minutes
            start_time (timestamp): Optional start time parameter. 
            end_time (timestamp): Optional end time parameter.
        """
        if not all(isinstance(database_id, int) for database_id in database_ids):
            raise ValueError("All elements of database_ids must be integers.")

        if not isinstance(replication_frequency_minutes, int):
            raise ValueError(f"replication_frequency_minutes should be an int, passed {replication_frequency_minutes}")

        if not (replication_frequency_minutes > 0 and replication_frequency_minutes < 8*1440):
            raise ValueError(f"replication_frequency_minutes must be in range [0, {8*1440}], passed value was {replication_frequency_minutes}")

        -- Default start time to 60 days ago if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=60)

            -- Default start time to 60 days ago if not provided
        if end_time is None:
            end_time = datetime.now()

        if start_time > end_time:
            raise ValueError(f"Start time {start_time} cannot be more than end time {end_time}.")
    
        return database_ids, replication_frequency_minutes, start_time, end_time

        

    database_ids, replication_frequency_minutes, start_time, end_time = validate_params(database_ids, replication_frequency_minutes, start_time, end_time)
    

    -- # Step 1: Get all tables in the database using SQL
    session = snowflake.snowpark.Session.builder.configs("<your_connection_params>").create()
    query = f"SELECT table_id, account_id, created_on FROM information_schema.tables WHERE database_id = {database_id}"
    tables = session.sql(query).collect()
    
    if not tables:
        return json.dumps({"error": f"Database not found for databaseId={database_id}"})
    
    added_files = 0
    added_bytes = 0
    database_logical_size = 0
    database_physical_size = 0
    histogram_deleted = {}

    -- # Step 2: Process each table's statistics
    for table in tables:
        table_id = table["table_id"]
        created_on = table["created_on"]

        # Simulate the calculation of physical and logical size (replace with your actual queries)
        logical_size = session.sql(f"SELECT SUM(logical_size) FROM <table_stat_table> WHERE table_id = {table_id}").collect()[0][0]
        physical_size = session.sql(f"SELECT SUM(physical_size) FROM <table_stat_table> WHERE table_id = {table_id}").collect()[0][0]

        database_logical_size += logical_size
        database_physical_size += physical_size

        # Simulate retrieval of histogram data and processing it
        # Add your logic to fetch histogram data and calculate `added_files`, `added_bytes`, `histogram_deleted`

    # Step 3: Compute churn rates for given frequencies
    files_bytes_result = []
    for freq in frequencies_min:
        # Add your logic to compute files and bytes for each frequency
        files = added_files  # Placeholder logic
        bytes = added_bytes  # Placeholder logic
        files_bytes_result.append({"frequency": freq, "files": files, "bytes": bytes})

    # Step 4: Prepare and return result
    result = {}
    if return_files_bytes:
        result["current database size"] = f"{database_physical_size} bytes"
        result["current database size with clone"] = f"{database_logical_size} bytes"

        for item in files_bytes_result:
            result[f"{item['frequency']} min"] = f"{item['files']} files, {item['bytes']} bytes"
    else:
        pivot = len(files_bytes_result) // 2
        while files_bytes_result[pivot]['bytes'] == 0 and pivot >= 0:
            pivot -= 1
        if pivot < 0:
            return json.dumps({"error": "Not enough data to calculate the cost. Please try again later!"})
        
        for item in files_bytes_result:
            cost = item['bytes'] / files_bytes_result[pivot]['bytes']
            result[f"{item['frequency']} min"] = cost

    return json.dumps(result)

$$;

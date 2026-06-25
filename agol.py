import time
import logging
import concurrent.futures
from datetime import datetime
from arcgis.gis import GIS
from arcgis.notebook import execute_notebook
from arcgis.features import Feature

# Configure standard logging to track pipeline execution in the console
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# GLOBAL VARIABLES FOR LOGGING
# ============================================================================
agol_logs = []
automationName = "AGOL Notebook Orchestrator"


# ============================================================================
# LOGGING UTILITIES
# ============================================================================
def record_log(level: str, message: str) -> None:
    """Logs to stdout and queues a Feature for AGOL table insertion."""
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)
    elif level == "WARNING":
        logging.warning(message)
        
    action_date_ms = int(time.time() * 1000)
    full_message = str(message)
    
    # Chunking logic to respect AGOL string field limits
    chunk_size = 980
    chunks = [full_message[i:i + chunk_size] for i in range(0, len(full_message), chunk_size)]
    total_chunks = len(chunks)
    
    for i, chunk in enumerate(chunks):
        final_text = f"(Part {i+1}/{total_chunks}) {chunk}" if total_chunks > 1 else chunk
        agol_logs.append(
            Feature(attributes={
                "actionDate": action_date_ms,
                "logLevel": level,
                "autoName": automationName,
                "actionResults": final_text
            })
        )

def push_logs_to_agol(log_table) -> None:
    """Pushes all queued logs to the AGOL logging table."""
    if not agol_logs:
        return
    try:
        logging.info(f"Pushing {len(agol_logs)} log records to AGOL...")
        log_table.edit_features(adds=agol_logs, rollback_on_failure=False)
        agol_logs.clear() # Clear the queue after successful push
        logging.info("Logs successfully pushed to AGOL.")
    except Exception as e:
        logging.error(f"Critical failure pushing to AGOL Log Service: {e}")

def get_last_sync_time(log_table) -> int:
    try:
        query = f"autoName = '{automationName}' AND logLevel = 'INFO' AND actionResults LIKE '%AWS Sync Complete%'"
        recent_log = log_table.query(where=query, order_by_fields="actionDate DESC", result_record_count=1).features
        
        if recent_log:
            last_date = recent_log[0].attributes.get('actionDate', 0)
            record_log("INFO", f"Last successful sync found at: {datetime.fromtimestamp(last_date/1000)}.")
            return last_date
    except Exception as e:
        record_log("WARNING", f"Could not determine last sync time. Defaulting to full push. Error: {e}")
    
    record_log("INFO", "No previous sync history found. Performing full baseline push.")
    return 0

# ============================================================================
# NOTEBOOK EXECUTION UTILITIES
# ============================================================================
def run_agol_notebook(gis: GIS, item_id: str, save_results: bool = False) -> dict:
    """Executes a target AGOL notebook by its item ID and waits for completion."""
    try:
        record_log("INFO", f"Retrieving Notebook Item: {item_id}")
        notebook_item = gis.content.get(item_id)
        
        if not notebook_item:
            raise ValueError(f"Could not locate item {item_id}. Verify the ID and permissions.")
        
        if notebook_item.type != 'Notebook':
            raise TypeError(f"Item {item_id} is not a Notebook (Type: {notebook_item.type}).")

        record_log("INFO", f"Initiating execution for: {notebook_item.title} ({item_id})")
        
        # 1. Pass future=True to handle the job asynchronously via the API
        exec_future = execute_notebook(notebook_item, update_portal_item=save_results, future=True)
        
        # 2. Call .result() to physically block the code until the server finishes the job
        record_log("INFO", f"Waiting for AGOL server to complete job for {item_id}...")
        final_response = exec_future.result() 
        
        # 3. Check the final response payload from AGOL
        status = final_response.get('status', '').lower()
        if 'fail' in status or 'error' in status:
            raise RuntimeError(f"AGOL server reported failure: {final_response}")
        
        record_log("INFO", f"SUCCESS: Finished execution for {item_id}. Server response: {status}")
        return {"status": "success", "item_id": item_id}

    except Exception as e:
        record_log("ERROR", f"FAILED: Notebook {item_id} encountered an error: {str(e)}")
        return {"status": "failed", "item_id": item_id, "error": str(e)}

# ============================================================================
# PIPELINE SEGMENTS
# ============================================================================
def execute_job1_segment(gis: GIS) -> None:
    segment_name = "Amazon as a Broker"
    record_log("INFO", f"--- Starting Segment Execution: {segment_name} ---")
    
    nb1_parallel = "Your notebook ID"
    nb2_parallel = "Your notebook ID"
    nb3_sequential = "Your notebook ID"
    
    parallel_results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_nb = {
            executor.submit(run_agol_notebook, gis, nb1_parallel): nb1_parallel,
            executor.submit(run_agol_notebook, gis, nb2_parallel): nb2_parallel
        }
        
        for future in concurrent.futures.as_completed(future_to_nb):
            nb_id = future_to_nb[future]
            try:
                result = future.result()
                parallel_results.append(result)
            except Exception as exc:
                record_log("ERROR", f"CRITICAL ERROR: Unhandled exception during ThreadPool execution for {nb_id}: {exc}")
                parallel_results.append({"status": "failed", "item_id": nb_id, "error": str(exc)})

    all_successful = all(res.get("status") == "success" for res in parallel_results)
    
    if not all_successful:
        record_log("WARNING", f"--- Segment Halted: {segment_name} ---")
        record_log("WARNING", "One or more prerequisite parallel notebooks failed. Skipping final sequential notebook.")
        return

    record_log("INFO", "Prerequisite parallel executions successful. Proceeding to the dependent notebook.")
    final_result = run_agol_notebook(gis, nb3_sequential)
    
    if final_result.get("status") == "success":
        record_log("INFO", f"--- Segment Completed Successfully: {segment_name} ---")
    else:
        record_log("ERROR", f"--- Segment Failed at Final Stage: {segment_name} ---")

def execute_job2_segment(gis: GIS) -> None:
    segment_name = "Database Mainteannce"
    record_log("INFO", f"--- Starting Segment Execution: {segment_name} ---")
    
    nb_run_task = "Your notebook ID"
    result = run_agol_notebook(gis, nb_run_task)
    
    if result.get("status") == "success":
        record_log("INFO", f"--- Segment Completed Successfully: {segment_name} ---")
    else:
        record_log("ERROR", f"--- Segment Failed: {segment_name} ---")

# ============================================================================
# MAIN ORCHESTRATOR EXECUTION
# ============================================================================
if __name__ == "__main__":
    gis = None
    log_table_item_id = "your feature service id"
    log_table = None

    try:
        # 1. Authenticate
        logging.info("Authenticating with ArcGIS Online via GIS('home')...")
        gis = GIS("home")
        
        # 2. Retrieve Log Table
        log_item = gis.content.get(log_table_item_id)
        if not log_item:
            raise ValueError(f"Failed to find Log Table item: {log_table_item_id}")
        
        # Assuming the log is a standalone table. 
        log_table = log_item.tables[0] 
        
        record_log("INFO", "Authentication successful. Log table connected.")
        record_log("INFO", "Starting Orchestrator Run.")

        # Optional: Check last sync time (using your provided utility)
        # get_last_sync_time(log_table)

        # 3. Execute Segments
        execute_job1_segment(gis)
        execute_job2_segment(gis)
        
        record_log("INFO", "Orchestrator script finished processing all scheduled segments.")

    except Exception as main_e:
        record_log("ERROR", f"Fatal Orchestrator Error: {str(main_e)}")
    
    finally:
        # 4. Push Logs to AGOL
        if log_table:
            push_logs_to_agol(log_table)
        else:
            logging.error("Could not push logs to AGOL. Log table was never instantiated.")





# AGOL Notebook Orchestrator

## Overview
The **AGOL Notebook Orchestrator** is a robust Python automation script designed to manage and execute multiple ArcGIS Online (AGOL) or ArcGIS Enterprise (AGE) Notebooks. It provides a resilient framework for running dependent notebooks in sequence, handling parallel executions using `concurrent.futures`, and centrally tracking pipeline health via a custom logging mechanism that writes directly to an ArcGIS Feature Service table.

This script is ideal for geospatial data pipelines, ETL tasks, and database maintenance operations that require strict dependency management and remote monitoring.

---

## Key Features
* **Centralized AGOL Logging:** Overrides standard local logging by capturing `INFO`, `WARNING`, and `ERROR` events and writing them to an AGOL Hosted Table.
* **Automatic Log Chunking:** Intelligently splits log messages exceeding 980 characters to respect AGOL string field limits, preventing data truncation or insertion errors.
* **Parallel Execution Execution:** Utilizes Python's `ThreadPoolExecutor` to run independent notebooks concurrently, saving processing time for non-dependent tasks.
* **Dependency Management:** Gracefully halts sequential pipelines if prerequisite parallel notebooks fail, ensuring data integrity.
* **Asynchronous Polling:** Leverages the ArcGIS API for Python's `execute_notebook(future=True)` method to physically block and wait for server-side job completion without timing out.
* **Native Authentication:** Built to run securely within the ArcGIS Notebooks environment using the native `GIS("home")` workspace token.

---

## Prerequisites

Before running this orchestrator, ensure the following items are configured in your ArcGIS organization:

1.  **ArcGIS Notebook Environment:** This script is designed to run inside an ArcGIS Notebook. If running locally, you will need to update the `GIS("home")` authentication to use explicit credentials or a profile.
2.  **Target Notebooks:** The target notebooks must exist in the portal, and the executing account must have permissions to access and run them.
3.  **Logging Feature Service Table:** An AGOL/AGE Hosted Feature Layer containing a standalone table to store logs. The table requires the following schema:
    * `actionDate` (Date/Time or Integer/Epoch)
    * `logLevel` (String)
    * `autoName` (String)
    * `actionResults` (String, length ~1000)

---

## Configuration

Before deploying, update the placeholders in the script with your specific item IDs:

### 1. Update the Logging Service ID
Locate the Main Orchestrator execution block and update the `log_table_item_id` with the Item ID of your logging feature service:



To be continued......

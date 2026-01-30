
# Task 3.1 Spark Architecture & ETL Flow
Utilizing Apache Spark, the pipeline ingests raw XML, resolves data quality issues (missing identifiers), merges planned schedules with live updates, and produces a Parquet dataset for delay analysis and traffic density queries.

**Running the Pipeline:**

After adjusting the resources specified in `docker-compose.yml` for the local machine:
ETL:
```
spark-submit \
  --master spark://spark-master:7077 \
  spark_batched.py
```

Queries for Task 3.2 and 3.3
```
spark-submit \
  --master spark://spark-master:7077 \
  spark_queries.py
```



The logic follows the same approach we implemented for the Task 1.2. Rewritten using built in Spark functions.

## Extraction

For the extraction of the xml files we use `sparkContext.wholeTextFiles`, to handle possible xml irregularities.
- **Batch Parsing:** The custom parsers (`parse_timetable_partition`, `parse_changes_partition`) run inside `mapPartitions`. This allows multiple XML files to be parsed in a single task execution, significantly reducing overhead compared to parsing files individually.
- **Attribute Extraction:** Logic uses Python's `xml.etree` to extract specific attributes and flatten the hierarchical XML into flat rows.
    

## Transformation (Data Cleaning & Resolution)

**A. Station Identity Resolution (Backfilling):**
It implements a simplified version of station name matching used in Task 1.2
Since timetable lacks the `station_eva` that we use as the primary key. This function implements a two-stage name matching against a look up table from the reference (`station_data.json`) to recover the station_eva:

1. **Normalizied Exact Match:** Station names are normalized (e.g., "Berlin-Tegel (S)" $\rightarrow$ "tegel s") to ensure consistent matching, using the same Python UDF `to_station_search_name()` for Task 1.2. Then direct join against the normalized lookup table.    
2. **Fuzzy Match:** For remaining unmatched records, a combined match score is applied (taking the max score after threshold filtering):
    - **Levenshtein Distance:** Measures character edits required to match strings. With threshold of (0.5 >= levenshtein distance normalized for string length)
    - **Jaccard Similarity:** Measures token overlap to prevent false positives (e.g., distinct stations with similar spellings that differ in letters like Schöneweide-Schöneberg, Schulzendorf-Zehlendorf), with 0.3 threshold, which might be a small value for noisier datasets.
        
--> Since the station lookup table is small, it is broadcasted to all nodes to perform this computationally expensive join efficiently without data shuffling.
        

**B. State Resolution (Merging Planned vs. Actual):**

Implemented in `resolve_latest_stop_state()`. A train stop may have one planned record and multiple update records over time.
- **Union:** The planned and changes DataFrames are unified into a single stream of events.
- **Windowing:** A Spark Window partitioned by `stop_id` and `station_eva` orders events by `snapshot_key` (time).
- **Coalescence:** The pipeline uses `last(ignorenulls=True)` to carry forward the most recent valid status update for every field (arrival time, cancellation status, platform).
- **Filtering:** "Changes-only" stops (updates with no corresponding planned schedule) are discarded unless explicitly marked as "added" stops.
    

## Load (Storage)

As per the task description pipeline writes the processed data (but also the input data namely `timetables`and `changes` to be able to manually verify the `final_movements` contents, it can/should be commented out for a deployed pipeline) in **Parquet** format.
Written Parquets are partitioned by `snapshot_date`.
Unlike `fact_movement`finalized table `movements/final_movements` that is to be used for queries contain only the resolved arrival/departure times, and not the every change made for the stop.

Possible enhancement would be to write the partitions like the dataset folder structure where we would have a intermediary folder for each week and inside the parquets with each day of that week. Since we already have larger tables (15 mins of each station vs 1 day of every station) we have decided to keep this simplified folder structure for the storage.

---

## Data Schema

### Input Schemas 
The name of the fields kept in shortened form for consistency with the xml contents.

| **Field**         | **Description**                                 |
| ----------------- | ----------------------------------------------- |
| `stop_id`         | Unique identifier for a specific stop sequence. |
| `station_eva`     | Station ID (often missing in raw data).         |
| `ar_pt` / `dp_pt` | Planned Arrival/Departure Time (YYMMDDHHmm).    |
| `ar_ct` / `dp_ct` | Changed (Real-time) Arrival/Departure Time.     |
| `ar_cs` / `dp_cs` | Status code (e.g., 'c' for cancelled).          |

<img src="input.webp" alt="input" width=700/>

### Output Schema (`final_movements`)

<img src="output.webp" alt="output" width=400/>

The final output is a flat, analytics-ready schema:
- **Keys:** `station_eva`, `stop_id`, `train_number`, `category`
- **Timestamps:**
    - `planned_arrival_ts`, `planned_departure_ts`
    - `actual_arrival_ts`, `actual_departure_ts` (Resolved: uses latest changed time if present, otherwise planned)
- **Flags:** `arrival_cancelled`, `departure_cancelled`, `arrival_is_hidden`
    

---

# Queries

The pipeline includes built-in analytical modules (Tasks 3.2 & 3.3) that operate on the resolved data.

**1. Average Daily Delay Calculation:**

Since exercise sheet did not specified the input format, we are assuming the input is station_eva.


- **Logic:** $Delay = Actual\_Time - Planned\_Time$.
    
- The pipeline differentiates between On Time (no change record) and Delayed. It calculates delay only when a changed time exists.
    
- **Exclusions:** Cancelled stops and "hidden" operational stops are excluded from the average.


**INSERT RESULT EXAMPLE**




**2. Peak-Hour Traffic Density:**

- **Logic:** Aggregates departures occurring during peak windows (07:00–09:00 and 17:00–19:00).
    
- **Aggregation:** Computes the average count per station per day. Importantly, it includes days with zero departures.

**INSERT RESULT EXAMPLE**







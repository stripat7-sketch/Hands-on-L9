# Hands-on-L9 - Ride Sharing Analytics using Spark Streaming

## Overview
In this project, I built a simple real-time data pipeline using PySpark Structured Streaming. The goal was to simulate ride-sharing data, process it as it arrives, and perform basic analytics like aggregations and time-based analysis.

The data is generated continuously using a Python script and then processed using Spark.

---

## Project Structure
```
ride-sharing-analytics/
├── outputs/
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
├── Task 1 Output.png
└── README.md
```

---

## Requirements

Before running the project, make sure you have:

- Python 3 installed  
- PySpark installed  
- Faker library installed  
- Java (version 11 or 17 works best with Spark)

Install required Python packages:
```bash
pip install pyspark faker
```
---

## How to Run

### 1. Start the Data Generator
First, run the script that generates streaming ride data:
```bash
python3 data_generator.py
```
This will continuously send JSON data to `localhost:9999`.

---

### 2. Task 1: Streaming Ingestion
```bash
python3 task1.py
```
What it does:
- Reads data from the socket
- Parses JSON into columns
- Displays the data in the console

### Note on Task 1 Output

Task 1 focuses only on streaming ingestion and parsing of data.  
The output is displayed in the console using Spark’s `writeStream.format("console")`.

No CSV files are generated for Task 1 as per the implementation.

---

### 3. Task 2: Driver-Level Aggregations
```bash
python3 task2.py
```
What it does:
- Groups data by `driver_id`
- Calculates:
  - Total fare per driver
  - Average trip distance per driver
- Saves results as CSV files in:
  outputs/task_2/

---

### 4. Task 3: Window-Based Analysis
```bash
python3 task3.py
```
What it does:
- Converts timestamp to proper format
- Applies a 5-minute window (sliding every 1 minute)
- Calculates total fare within each window
- Saves results as CSV files in:
  outputs/task_3/

---

## Output

- Task 1 → Data printed in console  
- Task 2 → CSV files generated per batch in `outputs/task_2/`  
- Task 3 → CSV files generated per batch in `outputs/task_3/`  

---

## Challenges Faced

While working on this project, I encountered a few issues:

- **Java Compatibility Issues**  
  Spark did not work properly with newer Java versions (like Java 24). I had to switch to Java 11 or Java 17 to make the streaming jobs run successfully.

- **Checkpoint Errors in Streaming**  
  Initially, the streaming jobs failed due to missing checkpoint locations. Adding a checkpoint directory in the `writeStream` fixed this issue.

- **Understanding Continuous Streaming Behavior**  
  At first, it was confusing that the program kept running and producing infinite batches. I later understood that Structured Streaming runs continuously and needs to be stopped manually.

- **Socket Connection Timing**  
  If the data generator was not running before the Spark jobs, the connection would fail. I learned that the data generator must always be started first.

---

## Important Notes

- The data generator must be running before starting any task.
- All streaming jobs run continuously.
- To stop any task, press:
  ```bash
  Ctrl + C
  ```

---

## Conclusion

This project helped me understand how real-time data processing works using Spark Structured Streaming. I learned how to read streaming data, process it, and perform both simple and window-based aggregations.

---















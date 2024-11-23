
The XCom-based approach eliminates the need for intermediate storage.
Experiment by modifying parameters or adding new tasks to extend the workflow.


## **Project: Parameterized DAGs Using XCom**


```markdown
# Parameterized DAGs with XCom in Apache Airflow (Practice Project)

## Overview
This project builds upon **parameterized DAGs** and introduces **XCom (cross-task communication)** in Apache Airflow.
 It shows how to share data between tasks dynamically in an ETL pipeline. 
This is a **practice project for beginners**, using mock data to simulate filtering, transforming, and merging customer and sales information.

## Features
- **Parameterized ETL**: Pass filtering parameters dynamically.
- **XCom Usage**: Share filtered data between tasks without saving intermediate results to disk.
- **Comprehensive Workflow**: Demonstrates task dependencies and data flow in Airflow.

## Dataset
The datasets used are:
- **`customer_data.csv`**: Mock customer data.
- **`sales_data.csv`**: Mock sales data.

## Requirements
- Apache Airflow
- Python 3.x
- Pandas
- NumPy

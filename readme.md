# Real-Time Industrial Sensor Data Pipeline

## Overview
This project is an automated, real-time data engineering pipeline designed to ingest, validate, transform, and store continuous time-series data from manufacturing sensors. 


## Architecture & Design
The pipeline is built with Python and follows a robust Extract, Transform, Load (ETL) architecture:



1. **Ingestion (Event-Driven):** Uses the `watchdog` library to continuously monitor a designated `data/` directory. When a simulated sensor batch arrives, the pipeline triggers instantly.

2. **Validation:** 
   * Incoming data is strictly validated using `pandas`.
   * Rejects batches with missing (null) values in key fields.
   * Enforces physical boundaries (e.g., Temperature between -50°C and 50°C, Humidity 0-100%).
   * Quarantines invalid files instantly, documenting the failure reason in an error log.

3. **Transformation & Analysis:**
   * Converts raw Fahrenheit temperature readings to Celsius.
   * Parses string timestamps into standardized `datetime` objects.
   * Calculates aggregated metrics (min, max, mean, standard deviation) grouped by machine.

4. **Storage (PostgreSQL):** Uses `SQLAlchemy` to automatically push the validated raw data and the aggregated metrics into structured relational tables.

## Key Features
* **Zero Data Loss & Fault Tolerance:** Implements a retry mechanism for database connection failures. If the database goes offline, files are safely routed to a `pending_retry/` directory.
* **Startup Sweep:** Automatically recovers and processes any files that accumulated while the pipeline service was offline.
* **Analytics-Ready:** The structured PostgreSQL backend is optimized for immediate connection to BI tools like Tableau or Power BI for downstream dashboarding.

## Database Schema
The pipeline automatically populates two core tables in the `sensor_db` PostgreSQL database:

### Table: `raw_sensor_data`
| Column | Description |

| `timestamp` | The exact time the sensor reading was recorded |
| `machine_id` | Unique identifier of the manufacturing equipment |
| `temperature` | Temperature reading in Celsius |
| `humidity` | Relative humidity percentage |
| `pressure` | Atmospheric pressure reading |

### Table: `aggregated_sensor_metrics`
| Column | Description |

| `machine_id` |  Unique identifier of the manufacturing equipment |
| `[metric]_min` | Minimum value of the metric in the processed batch |
| `[metric]_max` | Maximum value of the metric in the processed batch |
| `[metric]_mean` | Average value of the metric in the processed batch |
| `[metric]_std` | Standard deviation of the metric in the batch |
| `source_file` | The original filename for data lineage tracking |
| `inserted_at` | System timestamp of database insertion |

## Setup & Execution

### Prerequisites
* Python 3.8+
* PostgreSQL installed and running locally on port `5432`
* A local PostgreSQL database created and named `sensor_db`

### 1. Installation
Clone the repository and install the required dependencies:

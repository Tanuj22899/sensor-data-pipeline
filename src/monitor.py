import time
import os
import shutil
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine

# Paths
PROJECT_ROOT = os.path.abspath(os.getcwd())
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
QUARANTINE_DIR = os.path.join(PROJECT_ROOT, 'quarantine')
PENDING_DIR = os.path.join(PROJECT_ROOT, 'pending_retry') 

# Database Configuration
DB_USER = 'postgres'
DB_PASSWORD = '12345'  
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sensor_db'

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            print(f"\n[{time.strftime('%X')}]  New file detected: {os.path.basename(event.src_path)}")
            time.sleep(2) 
            process_single_file(event.src_path)

def process_single_file(filepath):
    try:
        clean_df = process_and_validate(filepath)
        
        if clean_df is not None:
            metrics_df = analyze_data(clean_df, filepath)
            success = push_to_db_with_retry(clean_df, metrics_df)
            
            if success:
                print(f"[{time.strftime('%X')}]  Success! Data inserted into database.")
                if os.path.exists(filepath):
                    os.remove(filepath) 
            else:
                filename = os.path.basename(filepath)
                pending_path = os.path.join(PENDING_DIR, filename)
                shutil.move(filepath, pending_path)
                log_error(filename, "Database offline. Moved to pending_retry.")
                print(f"[{time.strftime('%X')}] Pipeline safe. File saved to pending_retry folder.")
                
    except Exception as e:
        log_error(os.path.basename(filepath), f"Critical pipeline error: {str(e)}")
        print(f"[{time.strftime('%X')}]  Critical Error, recovery engaged. See error_log.txt.")

def push_to_db_with_retry(clean_df, metrics_df, max_retries=3, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[{time.strftime('%X')}]  Pushing data to PostgreSQL (Attempt {attempt})...")
            clean_df.to_sql('raw_sensor_data', engine, if_exists='append', index=False)
            metrics_df.to_sql('aggregated_sensor_metrics', engine, if_exists='append', index=False)
            return True 
        except Exception as e:
            print(f"[{time.strftime('%X')}]  DB Connection failed: {e}")
            if attempt < max_retries:
                print(f"[{time.strftime('%X')}]  Retrying in {delay} seconds...")
                time.sleep(delay)
    
    print(f"[{time.strftime('%X')}]  Max retries reached. Database is unavailable.")
    return False 

def process_and_validate(filepath):
    try:
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip().str.lower()
        
        required_columns = ['timestamp', 'machine_id', 'temperature', 'humidity', 'pressure']
        available_columns = [col for col in required_columns if col in df.columns]
        df = df[available_columns]
        
        if df.isnull().values.any():
            send_to_quarantine(filepath, "Failed: Contains null values.")
            return None 

        if 'temperature' in df.columns:
            df['temperature'] = (df['temperature'] - 32) * 5.0 / 9.0

        if 'temperature' in df.columns and not df['temperature'].between(-50, 50).all():
            send_to_quarantine(filepath, "Failed: Temperature out of range (-50C to 50C).")
            return None
            
        if 'humidity' in df.columns and not df['humidity'].between(0, 100).all():
            send_to_quarantine(filepath, "Failed: Humidity out of range (0 to 100).")
            return None
            
        if 'pressure' in df.columns and not df['pressure'].between(0, 10).all():
            send_to_quarantine(filepath, "Failed: Pressure out of range (0 to 10).")
            return None

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    except Exception as e:
        send_to_quarantine(filepath, f"Validation logic error: {str(e)}")
        return None

def analyze_data(df, filepath):
    numeric_cols = ['temperature', 'humidity', 'pressure']
    numeric_cols = [col for col in numeric_cols if col in df.columns]
    
    metrics = df.groupby('machine_id')[numeric_cols].agg(['min', 'max', 'mean', 'std']).reset_index()
    metrics.columns = ['_'.join(col).strip('_') for col in metrics.columns.values]
    
    metrics = metrics.fillna(0) 
    
    metrics['source_file'] = os.path.basename(filepath)
    metrics['inserted_at'] = pd.Timestamp.now()
    return metrics

def send_to_quarantine(filepath, reason):
    filename = os.path.basename(filepath)
    quarantine_path = os.path.join(QUARANTINE_DIR, filename)
    shutil.move(filepath, quarantine_path)
    log_error(filename, reason)
    print(f"[{time.strftime('%X')}]  Quarantined: {filename} -> {reason}")

def log_error(filename, reason):
    with open(os.path.join(PROJECT_ROOT, 'error_log.txt'), "a") as log_file:
        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] File: {filename} | Reason: {reason}\n")

def run_startup_sweep():
    print(f"[{time.strftime('%X')}]  Running startup sweep for missed files...")
    missed_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    if not missed_files:
        print(f"[{time.strftime('%X')}]  No missed files found.")
        return

    print(f"[{time.strftime('%X')}]  Found {len(missed_files)} missed file(s). Processing now...")
    for filename in missed_files:
        filepath = os.path.join(DATA_DIR, filename)
        process_single_file(filepath)

def start_monitoring():
    run_startup_sweep() 
    
    print(f"\n Monitoring '{DATA_DIR}' for incoming CSV files...")
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, path=DATA_DIR, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(5) 
    except KeyboardInterrupt:
        observer.stop()
        print("\nMonitoring stopped.")
    observer.join()

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    os.makedirs(PENDING_DIR, exist_ok=True) 
    
    start_monitoring()
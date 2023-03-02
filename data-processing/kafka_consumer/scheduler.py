import subprocess
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(filename='data_processing.log', level=logging.INFO)

def run_command(command):
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        logging.error(f"Command '{command}' failed with error: {error.decode()}")
    return output

def run_data_processing():
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hadoop_dir = os.path.join(project_dir, 'data_processing', 'hadoop_processing')

    # Run Hadoop
    try:
        run_command(f'cd {hadoop_dir} && ./run_hadoop.sh')
        logging.info('Hadoop processing completed')
    except Exception as e:
        logging.error(f"Hadoop processing failed with error: {str(e)}")

    # Process data with Spark
    spark_dir = os.path.join(project_dir, 'data_processing', 'spark_processing')
    try:
        run_command(f'spark-submit {spark_dir}/processing/consumer.py')
        logging.info('Spark processing completed')
    except Exception as e:
        logging.error(f"Spark processing failed with error: {str(e)}")

    # Load data into HBase
    hbase_dir = os.path.join(project_dir, 'data_processing', 'hbase_connection')
    try:
        run_command(f'cd {hbase_dir} && python connection.py')
        logging.info('Data loaded into HBase')
    except Exception as e:
        logging.error(f"Failed to load data into HBase with error: {str(e)}")

    # Clean up log files
    log_dir = os.path.join(project_dir, 'data_processing', 'log_files')
    try:
        run_command(f'rm {log_dir}/*.log')
        logging.info('Log files cleaned up')
    except Exception as e:
        logging.error(f"Failed to clean up log files with error: {str(e)}")

# Run data processing every 15 minutes
while True:
    now = datetime.now()
    if now.minute % 15 == 0:
        run_data_processing()
    next_run = (now + timedelta(minutes=15)).replace(second=0, microsecond=0)
    time_to_sleep = (next_run - now).seconds
    time.sleep(time_to_sleep)

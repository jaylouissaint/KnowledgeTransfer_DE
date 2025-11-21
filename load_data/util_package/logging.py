'''This file contains the function to append errors to an error log file'''
import logging
import os

# create log directory
# Folder to save missing data
folder = "error_log"
os.makedirs(folder, exist_ok=True)

# Create a descriptive file name
file_path = os.path.join(folder, "etl.log")

# Configure logging
logging.basicConfig(
    filename=file_path,
    filemode='a',   # append errors
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)


# Function to get a named logger for each module
def get_logger(name):
    return logging.getLogger(name)

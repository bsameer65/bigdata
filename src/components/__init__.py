import os
import sys
import logging

log_dir = "logs"
log_file = os.path.join(log_dir,"project_logs.log")
log_info = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level= logging.INFO,
    format= log_info,
    
    handlers= [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("BigDataProjectLogger")
import os
import sys
# from components.data_processing.get_data import DataProcessing
# from components import logger
# from components.hello import say_hello


# # Dynamically add 'src' folder to path
# src_path = os.path.join(os.path.dirname(__file__), 'src')
# if src_path not in sys.path:
#     sys.path.append(src_path)
    
src_path = os.path.join(os.path.dirname(__file__), 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

# ✅ Now import your modules
from components.data_processing.get_data import DataProcessing
from components import logger

STAGE_NAME = "Data Ingestion Stage"
try:
    logger.info(f">>>>>> satge {STAGE_NAME} staretd <<<<<<")
    processor= DataProcessing()
    processor.get_file()
    logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
        logger.exception(e)
        raise e






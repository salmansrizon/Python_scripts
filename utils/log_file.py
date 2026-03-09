import logging
import os
import datetime
import inspect
import sys
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
from typing import Optional

class LogManager:
    """Manager class for handling logging configuration and operations (Singleton per script)"""
    
    _instances = {}

    def __new__(cls, module_name: Optional[str] = None):
        # We uniquely identify the log file by the calling script's name
        frame = inspect.stack()[1]
        calling_script = os.path.basename(frame.filename).split('.')[0]
        
        if calling_script not in cls._instances:
            instance = super().__new__(cls)
            instance._init_logger(calling_script)
            cls._instances[calling_script] = instance
            
        return cls._instances[calling_script]
        
    def _init_logger(self, calling_script: str):
        """Perform the actual instance initialization only once per script run"""
        self.log_folder = "C://Python_scripts//log"
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)

        # Generate daily timestamp for log file name
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        
        # Create log file name using calling script name
        self.log_file = os.path.join(self.log_folder, f"{calling_script}_{timestamp}.log")
            
        # Configure logging
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )
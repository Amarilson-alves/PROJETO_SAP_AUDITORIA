import logging
from datetime import datetime
import os

def configurar_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    arquivo_log = f"logs/execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(arquivo_log),
            logging.StreamHandler() # Exibe no terminal também
        ]
    )
    return logging.getLogger("SAP_Audit")
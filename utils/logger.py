# utils/logger.py
import logging
import os
from datetime import datetime

def configurar_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    arquivo_log = f"logs/execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Adicionamos 'encoding="utf-8"' no FileHandler
    file_handler = logging.FileHandler(arquivo_log, encoding='utf-8')
    stream_handler = logging.StreamHandler()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[file_handler, stream_handler]
    )
    return logging.getLogger("SAP_Audit")
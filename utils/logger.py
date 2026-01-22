# utils/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler

def configurar_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger('SAP_Auditoria')
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # Rotação: 10MB por arquivo, mantém os últimos 5
    file_handler = RotatingFileHandler(
        'logs/auditoria.log', 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
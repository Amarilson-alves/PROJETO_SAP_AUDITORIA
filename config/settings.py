# config/settings.py
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

FILES = {
    'MB51': os.path.join(DATA_DIR, 'MB51.xlsx'),
    'MB52': os.path.join(DATA_DIR, 'MB52.xlsx'),
    'ALDREI': os.path.join(DATA_DIR, 'Aldrei.xlsx'),
    'SAIDA': os.path.join(OUTPUT_DIR, 'Resultado_Auditoria_PRO.xlsx')
}

OFFSET_DASHBOARD = 8  # Centraliza a posição de início dos dados
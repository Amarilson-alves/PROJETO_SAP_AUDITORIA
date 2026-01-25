# utils/settings.py
import yaml
import os

def carregar_config():
    caminho = 'config.yaml'
    if not os.path.exists(caminho):
        raise FileNotFoundError("Arquivo config.yaml não encontrado na raiz.")
    
    with open(caminho, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)
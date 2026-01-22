# 🚀 Motor de Auditoria SAP PRO - Projeto Pia do Sul

Este sistema automatiza a conciliação e auditoria de estoques (AMED) utilizando dados do SAP (MB51/MB52) e controles de campo (Aldrei).

## 📊 Principais Funcionalidades
- **Normalização Inteligente**: Tratamento de strings com Unicode para saneamento de frentes (B2B, Site, Manutenção).
- **Algoritmo de Compensação**: Sugestão automática de aplicação entre IDs com sobra e IDs com falta (Livro Razão).
- **Dashboard Executivo**: Geração de capa de indicadores financeiros e operacionais integrada no Excel.
- **Arquitetura Modular**: Separação clara entre Leitura, Validação, Regras de Negócio e Formatação.

## ⚙️ Como Executar
1. Instale as dependências: `pip install pandas xlsxwriter calamine openpyxl`
2. Insira as bases na pasta `/data` (MB51.xlsx, MB52.xlsx, Aldrei.xlsx).
3. Execute o script principal: `python main.py`
4. Verifique o resultado e o dashboard na pasta `/output`.
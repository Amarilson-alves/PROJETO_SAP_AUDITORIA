# 🚀 Motor de Auditoria SAP PRO - Projeto Pia do Sul

Este projeto consiste em um motor de auditoria automatizado desenvolvido em Python para a reconciliação de estoques e aplicações de materiais (AMED). O sistema cruza dados operacionais extraídos do SAP (transações MB51 e MB52) com planilhas de controle de campo (Aldrei) para identificar divergências e sugerir ações corretivas.



## 🛠️ Arquitetura e Diferenciais Técnicos

O projeto foi construído seguindo padrões de **Engenharia de Dados Defensiva**, garantindo resiliência mesmo diante de bases de dados inconsistentes:

* **Busca Dinâmica de Cabeçalho**: Implementação de algoritmos de busca para localizar tabelas dentro de arquivos Excel, ignorando linhas vazias ou metadados irrelevantes no topo do documento.
* **Normalização Unicode**: Tratamento de strings para remover acentos e caracteres especiais, garantindo a integridade do saneamento de frentes de trabalho (ex: B2B, Implantação, Site).
* **Motor de Auditoria (Livro Razão)**: Lógica avançada que compensa automaticamente sobras em determinados IDs com faltas em outros, respeitando regras de UF e Parceiro (Aliado).
* **Contrato de Interface (Schema Enforcement)**: Sistema de validação rigoroso que garante a existência de colunas críticas antes do processamento, evitando falhas na geração do dashboard.
* **Dashboard Executivo**: Geração automática de uma camada de indicadores (KPIs) no topo do relatório Excel, com formatação condicional dinâmica e filtros automáticos.

## 📋 Regras de Negócio Implementadas

1.  **Saneamento de Frentes**: Classificação automática baseada na coluna `OBRA` (regra SOBREP) e mapeamento De-Para para padronização de tipos de projeto.
2.  **Cálculo de Saldo por Aliado**: Diferenciação lógica de regras de saldo para "VIVO INSOURCING" versus demais parceiros do projeto.
3.  **Resultado Operacional**: Diagnóstico automático em cinco níveis: `OK`, `Aplicação Pendente`, `Aplicação Externa`, `Estorno` e `ID Cancelado`.

## 🚀 Como Executar

### Pré-requisitos
* Python 3.10+
* Bibliotecas: `pandas`, `xlsxwriter`, `python-calamine`, `openpyxl`

### Para instalar os requisitos:
* pip install -r requirements.txt

### Passo a Passo
1.  Clone o repositório para sua máquina local.
2.  Certifique-se de que a estrutura de pastas `/data`, `/output` e `/logs` existe (o sistema criará automaticamente na primeira execução).
3.  Insira os arquivos base na pasta `/data`:
    * `MB51.xlsx` (Histórico operacional)
    * `MB52.xlsx` (Saldo atual)
    * `Aldrei.xlsx` (Controle de campo)
4.  Execute o script principal:
    ```bash
    python main.py
    ```
5.  Consulte o resultado formatado em `/output` e o histórico detalhado em `/logs`.

---
*Desenvolvido para automação de processos de auditoria SAP.*
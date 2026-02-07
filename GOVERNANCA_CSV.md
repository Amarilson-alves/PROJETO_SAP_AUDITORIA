# 🛡️ Governança de Regras de Negócio (Arquivo Mestre)

## 1. O Arquivo Mestre (`data/dim_movimentos.csv`)
O robô de auditoria é **"Data-Driven"**. Ele não possui regras fixas ("hardcoded") em seu código fonte para determinar a natureza de um movimento. 

Todas as definições sobre o que constitui uma "Entrada", "Saída", "Estorno" ou "Consumo" residem exclusivamente no arquivo `dim_movimentos.csv`.

### Estrutura do Arquivo
* **BWART:** Código do Movimento SAP (Chave).
* **SENTIDO_AMED:** Define a matemática do saldo (`ENTRADA`, `SAIDA`, `NEUTRO`).
* **TIPO_ESPECIAL:** Define a regra de negócio (`ESTORNO`, `CONSUMO`, `COMPRA`, etc.).

## 2. Controle de Mudanças
Qualquer alteração neste arquivo altera o resultado da auditoria financeira e os indicadores de risco. Portanto:

1.  **Acesso:** Restrito ao Gestor do Processo e Auditor Sênior.
2.  **Versionamento:** Toda alteração deve ser salva (commit) com data e motivo.
3.  **Proibição:** É vetada a alteração de classificações (ex: mudar um 261 de SAÍDA para ENTRADA) para "maquiar" resultados de indicadores.

## 3. Novos Movimentos SAP
Caso a TI crie novos Tipos de Movimento (ex: Z01, Y50), o robô os ignorará (classificando como `NEUTRO`) até que sejam oficialmente cadastrados e classificados neste arquivo.
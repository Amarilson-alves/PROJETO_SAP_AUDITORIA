# 📊 Auditoria Contínua de Estoques AMED - Visão Geral

## O Problema Original
A gestão tradicional via MB52 (foto do saldo estático) mascarava três problemas crônicos na operação:
1.  **Furos de Estoque:** Materiais consumidos no sistema sem entrada prévia correspondente.
2.  **Estornos "Frios":** Entradas manuais (ex: 262) usadas para ajustar saldo sem lastro físico real.
3.  **Materiais Órfãos:** Estoque alocado no AMED sem dono (ID Recebedor) identificado.

## A Solução Tecnológica (v13.0)
Implementamos um **Motor de Auditoria Forense em Python** que reconstrói a história de cada item individualmente, utilizando lógica de pilha (LIFO) e validação cruzada.



### Principais Funcionalidades
* **Rastreabilidade Total:** Reconstrução cronológica de entradas e saídas por Lote e ID.
* **Detecção de Fraude/Erro:** Algoritmo capaz de identificar estornos realizados sem consumo anterior.
* **Aging Real:** Cálculo exato de dias parados baseado na data física da entrada, não na data contábil do saldo.

## Ganhos Imediatos
* **Precisão:** Identificação automática de itens parados há mais de 90 dias (*Aging Real*).
* **Compliance:** Bloqueio lógico de procedimentos irregulares (ex: estorno sem consumo).
* **Financeiro:** Mapeamento exato de valores em risco de perda ou desvio (Risco Monetário).
* **Velocidade:** Análise de 100% da base (+400k linhas) em minutos, substituindo a amostragem manual.
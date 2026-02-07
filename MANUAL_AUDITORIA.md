# 📘 Manual de Critérios: Auditoria Automatizada de Estoque (AMED)

**Versão:** 1.0  
**Responsável:** Auditoria Interna / Controle de Estoque  
**Escopo:** Materiais de Aplicação Direta e Estoques de Terceiros (AMED)

---

## 1. Objetivo
Estabelecer a metodologia lógica utilizada pelo **Motor de Auditoria Contínua (v13.0)** para a reconstrução histórica, cálculo de *aging* e identificação de irregularidades nos estoques AMED.

## 2. Metodologia de Reconstrução de Saldo
O sistema não utiliza o saldo estático do SAP (MB52) como única fonte da verdade. Ele realiza uma **Reconstrução Forense** baseada nos seguintes princípios:

### 2.1. Princípio da Pilha Física (Stack LIFO)
Para fins de rastreabilidade física e cálculo de *aging* (envelhecimento), o sistema adota o método **LIFO Técnico (Last In, First Out)**.
* **Regra:** As saídas (consumos) abatem sempre as entradas mais recentes disponíveis na pilha do material.
* **Justificativa:** Adota-se o **Princípio do Conservadorismo**. Ao consumir o estoque mais novo, o saldo remanescente representa o estoque mais antigo (pior cenário de aging), garantindo que materiais estagnados sejam evidenciados com prioridade.

### 2.2. Definição de Entradas e Saídas
A classificação dos movimentos (*BWART*) é gerida externamente via arquivo mestre (`data/dim_movimentos.csv`), garantindo a segregação entre a regra de negócio e o código fonte.

## 3. Matriz de Risco e Scoring
Cada item auditado recebe uma pontuação de risco (**SCORE_RISCO**, de 0 a 100) baseada na gravidade da irregularidade:

| Score | Classificação | Critério Detalhado | Ação Requerida |
| :--- | :--- | :--- | :--- |
| **100** | **CRÍTICO (Grave)** | Material sem ID (Órfão) OU Consumo sem lastro (Furo). | Regularização imediata / Estorno. |
| **95** | **ERRO SISTÊMICO** | Divergência entre Saldo Reconstruído e MB52. | Abertura de chamado TI/Contábil. |
| **90** | **PROCEDIMENTO** | Estorno (262/222) realizado sem consumo prévio válido. | Estorno da entrada indevida. |
| **80** | **ESTAGNADO** | Material parado há > 90 dias (*Aging*). | Devolução ao CD. |
| **60** | **ALERTA** | Retorno de Obra ou Transformação (309) sem baixa. | Reaplicação em nova obra. |
| **0-40**| **PENDENTE** | Fluxo normal de compra ou transferência recente. | Monitoramento. |

## 4. Limitações Técnicas Conhecidas
* **Granularidade da Prova Real (MB52):** A transação MB52 do SAP fornece saldos por *Material/Centro/Depósito*, enquanto a auditoria reconstrói saldos por *ID/Lote*. Pequenas divergências de arredondamento ou alocação entre IDs podem gerar alertas de "Divergência Sistêmica", que devem ser analisados caso a caso.
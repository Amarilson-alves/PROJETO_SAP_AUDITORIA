# Motor de Auditoria SAP MM - AMED 🚀

Solução robusta desenvolvida em Python para conciliação de estoques e auditoria de aplicação de materiais (AMED), integrando dados das transações SAP **MB51** e **MB52**.

## 🛠️ Tecnologias
- **Python 3.x**
- **Pandas** (Processamento de dados)
- **Calamine** (Leitura ultra-rápida de Excel)
- **XlsxWriter** (Formatação de relatórios profissionais)

## 📁 Estrutura
- `/core`: Inteligência de negócio e regras MM.
- `/utils`: Formatadores, loggers e auxiliares.
- `/data`: Arquivos de entrada (MB51, MB52, Aldrei).
- `/output`: Relatórios finais auditáveis.

## 📊 Funcionalidades
- Separação automática entre fluxo (MB51) e saldo (MB52).
- Motor de sugestão para compensação de saldos entre IDs de projeto.
- Logs detalhados de execução para defesa em auditoria.
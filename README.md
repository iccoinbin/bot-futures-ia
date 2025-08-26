# Bot de Futures com IA — Fase 0 (MVP seguro)

Esta versão contém:
- Conexão com **Binance Futures Testnet**
- Scripts para baixar candles, calcular **EMA/ATR**, e rodar um **sanity backtest**
- Kernel de risco básico e executor simulado
- Arquivos de configuração `.env`, `settings.yml` e `risk.yml`

## Como usar (resumo)
1) Crie um ambiente Python 3.10+ e instale `requirements.txt`
2) Copie `.env.example` para `.env` e preencha as chaves da **Testnet**
3) Rode os scripts em `src/scripts` na ordem 01 → 05

**Atenção:** nada de dinheiro real nesta fase.

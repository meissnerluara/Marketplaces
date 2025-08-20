# marketplace-backend

Backend FastAPI para coleta, tratamento e exportação de dados dos marketplaces Amazon, Magalu e Mercado Livre.

## Funcionalidades
- Autenticação por senha
- Coleta de dados dos marketplaces
- Processamento e validação de dados
- Exportação de relatórios em ZIP

## Estrutura
- `app/services/`: Serviços de integração e tratamento de dados
- `app/routes.py`: Rotas da API
- `app/main.py`: Inicialização do FastAPI

## Segurança
- Não expõe variáveis sensíveis
- CORS restrito ao domínio do frontend

Para dúvidas ou sugestões, entre em contato com o time de desenvolvimento da E-Consultoria de Vendas.

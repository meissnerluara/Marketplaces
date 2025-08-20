# Marketplaces Data Collector

Repositório contendo **backend e frontend** para coleta, tratamento e exportação de dados dos marketplaces **Amazon, Mercado Livre e Magalu**.  
O backend é responsável pelo processamento e exportação dos dados, enquanto o frontend oferece uma interface estática para iniciar a coleta e baixar relatórios.

---

## Funcionalidades

### Backend
- Coleta de dados dos marketplaces  
- Processamento e validação de dados  
- Exportação de relatórios em ZIP  

### Frontend
- Login por senha  
- Seleção de plataforma e vendedor  
- Início da coleta e acompanhamento dos logs  
- Download dos relatórios gerados  

---

## Estrutura do Repositório

### Backend (`/backend`)
- `app/main.py`: Inicialização do FastAPI  
- `app/routes.py`: Rotas da API  
- `app/services/`: Serviços de integração e tratamento de dados  

### Frontend (`/frontend`)
- `index.html`: Página principal  
- `icons/`: Ícones das plataformas  
- `vercel.json`: Configuração de deploy  

> O backend deve estar rodando e acessível para o funcionamento completo do frontend.

---

## Observação
- Este repositório tem finalidade **exclusivamente demonstrativa**, não sendo utilizado em ambiente de produção nem para deploy da aplicação.  

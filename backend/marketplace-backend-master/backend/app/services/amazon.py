import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values
import io
import zipfile
import pytz
import requests.exceptions
import time

# ------------------------- VARIÁVEIS DE AMBIENTE ----------------------------

load_dotenv()
auth_url = os.getenv("AMAZON_URL_BASE_AUTH")
client_id = os.getenv("AMAZON_CLIENT_ID")
client_secret = os.getenv("AMAZON_CLIENT_SECRET")
base_url = os.getenv("AMAZON_URL_BASE_API")
marketplace_id = os.getenv("AMAZON_MARKETPLACE_ID")

# ------------------------- CONFIGURAÇÃO BANCO DE DADOS ----------------------------

# Conexão com o banco de dados PostgreSQL
def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("AMAZON_DB_HOST"),
            port=os.getenv("AMAZON_DB_PORT"),
            dbname=os.getenv("AMAZON_DB_NAME"),
            user=os.getenv("AMAZON_DB_USER"),
            password=os.getenv("AMAZON_DB_PASSWORD"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        print("Erro ao conectar com o banco de dados no Supabase:", e)
        return None

# ------------------------- TOKENS ----------------------------

# Carrega os tokens do ambiente
def load_tokens():
    tokens_env = os.getenv("AMAZON_TOKENS")
    if tokens_env:
        try:
            data = json.loads(tokens_env)
            if not isinstance(data, dict):
                return {}
            return data
        except Exception:
            return {}
    return {}

# Obtém o access_token a partir do refresh_token
def get_access_token(refresh_token):
    url = os.getenv("AMAZON_URL_BASE_AUTH")
    client_id = os.getenv("AMAZON_CLIENT_ID")
    client_secret = os.getenv("AMAZON_CLIENT_SECRET")
    data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(url, data=data, headers=headers)
    if response.status_code == 200:
        token = response.json().get('access_token')
        return token
    else:
        print(f"Erro ao obter access_token:: Status {response.status_code}")
        return None

# ------------------------- CHAMADAS API ----------------------------

# Fazer requisições à API da Amazon
def make_request(url, headers, params=None, method="GET", timeout=30):
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
        elif method == "POST":
            response = requests.post(url, headers=headers, data=params, timeout=timeout)
        else:
            raise ValueError("Método HTTP não suportado.")
        if response.status_code == 200:
            return response
        else:
            print(f"Requisição falhou — status {response.status_code}")
            print(f"Resposta:: Status {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print("Erro na requisição:", e)
        return None

# Obtém todos os produtos
def get_listing_items(access_token, seller_id):
    url = f"{base_url}/listings/2021-08-01/items/{seller_id}"
    created_after = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    all_items = []
    page_token = None
    base_params = {
        "marketplaceIds": marketplace_id,
        "sortBy": "lastUpdatedDate",
        "createdAfter": created_after
    }
    while True:
        req_params = base_params.copy()
        if page_token:
            req_params["pageToken"] = page_token
        response = make_request(url, headers, params=req_params, method="GET", timeout=30)
        if response is None:
            break
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            validos = []
            for item in items:
                sku = item.get("sku")
                summaries = item.get("summaries", [])
                asin = None
                if summaries and isinstance(summaries, list) and summaries:
                    asin = summaries[0].get("asin")
                if sku or asin:
                    validos.append(item)
            if not validos:
                print("Fim da paginação: página sem nenhum sku ou asin.")
                break
            all_items.extend(validos)
            page_token = data.get("pagination", {}).get("nextToken")
            if not page_token:
                break
            time.sleep(2)
        else:
            print(f"Erro ao obter produtos:: Status {response.status_code}")
            break
    return all_items

def get_orders(access_token):
    created_after = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    url = f"{base_url}/orders/v0/orders"
    headers = {
        'Accept': 'application/json',
        'x-amz-access-token': access_token
    }
    params = {
        'CreatedAfter': created_after,
        'MarketplaceIds': marketplace_id
    }
    all_orders = []
    next_token = None
    while True:
        req_params = params.copy()
        if next_token:
            req_params = {'MarketplaceIds': marketplace_id, 'NextToken': next_token}
        response = make_request(url, headers, params=req_params, method="GET", timeout=180)
        if response is None:
            break
        if response.status_code == 200:
            data = response.json()
            payload = data.get('payload', {})
            orders = payload.get('Orders', [])
            all_orders.extend(orders)
            next_token = payload.get('NextToken')
            if not next_token:
                break
            time.sleep(2)
        else:
            print(f"Erro ao obter pedidos:: Status {response.status_code}")
            break
    return all_orders

def get_fba_inventory_summaries(access_token):
    start_date = (datetime.now(timezone.utc) - timedelta(days=90)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    url = f"{base_url}/fba/inventory/v1/summaries"
    headers = {
        'Accept': 'application/json',
        'x-amz-access-token': access_token
    }
    base_params = {
        'marketplaceIds': marketplace_id,
        'details': 'true',
        'granularityType': 'Marketplace',
        'granularityId': marketplace_id,
        'startDateTime': start_date
    }
    all_summaries = []
    next_token = None
    while True:
        req_params = base_params.copy()
        if next_token:
            req_params['nextToken'] = next_token
        response = make_request(url, headers, params=req_params, method="GET", timeout=30)
        if response is None:
            break
        data = response.json()
        payload = data.get('payload', {})
        summaries = payload.get('inventorySummaries', [])
        all_summaries.extend(summaries)
        next_token = data.get('pagination', {}).get('nextToken')
        if not next_token:
            break
        time.sleep(2)
    return all_summaries

def get_order_metrics(access_token):
    interval_start = (datetime.now(timezone.utc) - timedelta(days=365)).strftime('%Y-%m-%dT00:00:00Z')
    interval_end = datetime.now(timezone.utc).strftime('%Y-%m-%dT23:59:59Z')
    interval = f"{interval_start}--{interval_end}"
    url = f"{base_url}/sales/v1/orderMetrics"
    headers = {
        'Accept': 'application/json',
        'x-amz-access-token': access_token
    }
    params = {
        'marketplaceIds': marketplace_id,
        'interval': interval,
        'granularityTimeZone': 'America/Sao_Paulo',
        'granularity': 'Month'
    }
    all_metrics = []
    response = make_request(url, headers, params=params, method="GET", timeout=30)
    if response and response.status_code == 200:
        data = response.json()
        payload = data.get('payload', [])
        if isinstance(payload, list):
            all_metrics.extend(payload)
    else:
        print(f"Erro ao obter métricas de pedidos: Status {response.status_code}" if response else "Sem resposta")
    return all_metrics

# ------------------------- TRATAMENTO DE DADOS ----------------------------

# Função utilitária para remover timezone dos DataFrames
def remover_timezone_df(df):
    for col in df.select_dtypes(include=['datetimetz']).columns:
        df[col] = df[col].dt.tz_localize(None)
    return df

def traduzir_status_pedido(status):
    mapa = {
        "Canceled": "Cancelado",
        "Shipped": "Enviado",
        "Pending": "Pendente"
    }
    if not status:
        return "Não informado"
    return mapa.get(status, status)

def traduzir_detalhes_pagamento(detalhes):
    if not detalhes:
        return "Não informado"
    mapa = {
        "Debit": "Débito",
        "Installments": "Parcelado",
        "CreditCard": "Cartão de crédito",
        "Rewards": "Recompensas",
        "GiftCertificate": "Vale-presente",
        "Other": "Outro"
    }
    if isinstance(detalhes, list):
        return ", ".join([mapa.get(d, d) for d in detalhes])
    return mapa.get(detalhes, detalhes)

def traduzir_tipo_produto(tipo):
    mapa = {
        "SHORTS": "Bermuda",
        "PANTS": "Calça",
        "UNDERPANTS": "Roupa de baixo",
        "BRA": "Sutiã",
        "APPAREL": "Vestuário",
        "SKIRT": "Saia",
        "COORDINATED_OUTFIT": "Conjunto",
        "SHIRT": "Camisa",
        "BASE_LAYER_APPAREL_SET": "Roupa térmica",
        "ELECTRONIC_CABLE": "Cabo eletrônico",
        "CHARGING_ADAPTER": "Carregador",
        "CAMERA_CONTINUOUS_LIGHT": "Luz para câmera",
        "SWIMWEAR": "Roupa de banho",
        "SHOES": "Calçado",
        "CELLULAR_PHONE_CASE": "Capa de celular",
        "ELECTRONIC_ADAPTER": "Adaptador eletrônico",
        "PORTABLE_ELECTRONIC_DEVICE_COVER": "Capa para dispositivo portátil",
        "HEADPHONES": "Fone de ouvido",
        "SOCKS": "Meia",
        "MULTIPORT_HUB": "Hub USB",
        "COMPUTER_DRIVE_OR_STORAGE": "Armazenamento/Drive de computador"
    }
    return mapa.get(tipo, tipo or "Não informado")

def traduzir_tipo_condicao(cond):
    if cond == "new_new":
        return "Novo"
    if not cond:
        return "Não informado"
    return cond

def traduzir_status_produto(status):
    mapa = {
        "BUYABLE": "Disponível para venda",
        "DISCOVERABLE": "Visível no catálogo"
    }
    if not status:
        return "Não informado"
    if isinstance(status, list):
        return ", ".join([mapa.get(s, s) for s in status])
    return mapa.get(status, status)

# Padroniza os dados dos produtos
def tratar_dados_produtos(produtos, vendedor, data_consultada=None):
    produtos_tratados = []
    for p in produtos:
        summary = p.get("summaries", [{}])[0]
        main_image = summary.get("mainImage", {})
        produto = {
            "asin": summary.get("asin"),
            "sku": p.get("sku"),
            "tipo_produto": traduzir_tipo_produto(summary.get("productType")),
            "tipo_condicao": traduzir_tipo_condicao(summary.get("conditionType")),
            "status": traduzir_status_produto(summary.get("status")),
            "nome_item": summary.get("itemName"),
            "data_criacao": summary.get("createdDate"),
            "data_atualizacao": summary.get("lastUpdatedDate"),
            "imagem_url": main_image.get("link") if main_image.get("link") is not None else "Sem imagem",
            "imagem_largura": main_image.get("width") if main_image.get("width") is not None else 0,
            "imagem_altura": main_image.get("height") if main_image.get("height") is not None else 0,
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_consultada": data_consultada.replace(tzinfo=None) if data_consultada else None
        }
        produtos_tratados.append(produto)
    return produtos_tratados

# Padroniza os dados dos pedidos
def tratar_dados_pedidos(pedidos, vendedor, data_consultada=None):
    pedidos_tratados = []
    for p in pedidos:
        status_traduzido = traduzir_status_pedido(p.get("OrderStatus"))
        cancelado = status_traduzido == "Cancelado"
        pendente = status_traduzido == "Pendente"
        pedido = {
            "id_pedido": p.get("AmazonOrderId"),
            "municipio_comprador": p.get("BuyerInfo", {}).get("BuyerCounty") if p.get("BuyerInfo", {}).get("BuyerCounty") and p.get("BuyerInfo", {}).get("BuyerCounty") != "----------" else "Não informado",
            "status": status_traduzido,
            "data_compra": p.get("PurchaseDate"),
            "data_aprovacao": p.get("LastUpdateDate"),
            "canal_venda": p.get("SalesChannel"),
            "canal_fulfillment": p.get("FulfillmentChannel"),
            "detalhes_pagamento": traduzir_detalhes_pagamento(p.get("PaymentMethodDetails")),
            "total_pedido": "Pedido cancelado" if cancelado else "Pendente" if pendente else p.get("OrderTotal", {}).get("Amount"),
            "moeda": "Pedido cancelado" if cancelado else "Pendente" if pendente else p.get("OrderTotal", {}).get("CurrencyCode"),
            "itens_enviados": p.get("NumberOfItemsShipped"),
            "itens_nao_enviados": p.get("NumberOfItemsUnshipped"),
            "prime": p.get("IsPrime"),
            "pedido_empresarial": p.get("IsBusinessOrder"),
            "estado_entrega": "Pedido cancelado" if cancelado else "Pendente" if pendente else p.get("ShippingAddress", {}).get("StateOrRegion"),
            "cidade_entrega": "Pedido cancelado" if cancelado else "Pendente" if pendente else p.get("ShippingAddress", {}).get("City"),
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_consultada": data_consultada.replace(tzinfo=None) if data_consultada else None
        }
        pedidos_tratados.append(pedido)
    return pedidos_tratados

# Padroniza os dados do estoque
def tratar_dados_estoque(estoque, vendedor, data_consultada=None):
    estoque_tratado = []
    for e in estoque:
        item = {
            "asin": e.get("asin"),
            "fnsku": e.get("fnSku"),
            "condicao": e.get("condition"),
            "disponivel_vendavel": e.get("inventoryDetails", {}).get("fulfillableQuantity"),
            "recebendo_em_estoque": e.get("inventoryDetails", {}).get("inboundReceivingQuantity"),
            "reservado_total": e.get("inventoryDetails", {}).get("reservedQuantity", {}).get("totalReservedQuantity"),
            "reservado_cliente": e.get("inventoryDetails", {}).get("reservedQuantity", {}).get("pendingCustomerOrderQuantity"),
            "reservado_transito": e.get("inventoryDetails", {}).get("reservedQuantity", {}).get("pendingTransshipmentQuantity"),
            "reservado_processamento": e.get("inventoryDetails", {}).get("reservedQuantity", {}).get("fcProcessingQuantity"),
            "em_pesquisa_total": e.get("inventoryDetails", {}).get("researchingQuantity", {}).get("totalResearchingQuantity"),
            "pesquisa_curto_prazo": 0,
            "pesquisa_medio_prazo": 0,
            "pesquisa_longo_prazo": 0,
            "inutilizavel_total": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("totalUnfulfillableQuantity"),
            "inutilizavel_danificado_cliente": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("customerDamagedQuantity"),
            "inutilizavel_danificado_armazem": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("warehouseDamagedQuantity"),
            "inutilizavel_danificado_distribuidor": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("distributorDamagedQuantity"),
            "inutilizavel_danificado_transportadora": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("carrierDamagedQuantity"),
            "inutilizavel_defeituoso": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("defectiveQuantity"),
            "inutilizavel_vencido": e.get("inventoryDetails", {}).get("unfulfillableQuantity", {}).get("expiredQuantity"),
            "fornecimento_futuro_reservado": e.get("inventoryDetails", {}).get("futureSupplyQuantity", {}).get("reservedFutureSupplyQuantity"),
            "fornecimento_futuro_compravel": e.get("inventoryDetails", {}).get("futureSupplyQuantity", {}).get("futureSupplyBuyableQuantity"),
            "nome_produto": e.get("productName"),
            "quantidade_total": e.get("totalQuantity"),
            "ultima_atualizacao": e.get("lastUpdatedTime"),
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_consultada": data_consultada.replace(tzinfo=None) if data_consultada else None
        }
        estoque_tratado.append(item)
    return estoque_tratado

# Padroniza os dados do faturamento
def tratar_dados_faturamento(faturamento, vendedor):
    faturamento_tratado = []
    for f in faturamento:
        item = {
            "periodo_inicio": f.get("interval", "").split("--")[0],
            "periodo_fim": f.get("interval", "").split("--")[-1] if "--" in f.get("interval", "") else None,
            "unidades_vendidas": f.get("unitCount"),
            "itens_vendidos": f.get("orderItemCount"),
            "pedidos": f.get("orderCount"),
            "preco_medio_unitario": f.get("averageUnitPrice", {}).get("amount"),
            "moeda_unitario": f.get("averageUnitPrice", {}).get("currencyCode"),
            "total_vendas": f.get("totalSales", {}).get("amount"),
            "moeda_vendas": f.get("totalSales", {}).get("currencyCode"),
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None)
        }
        faturamento_tratado.append(item)
    return faturamento_tratado

# Padroniza os erros de qualidade dos produtos
def tratar_erros_qualidade_produtos(produtos, vendedor, data_consultada=None):
    erros = []
    for p in produtos:
        erro = {
            "asin": p.get("asin"),
            "sku": p.get("sku"),
            "titulo": p.get("nome_item"),
            "status": traduzir_status_produto(p.get("status")),
            "url_imagem_principal": p.get("imagem_url"),
            "resolucao_imagem": "OK" if p.get("imagem_largura") and p.get("imagem_altura") and p.get("imagem_largura") >= 500 and p.get("imagem_altura") >= 500 else "Resolução baixa",
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_consultada": data_consultada.replace(tzinfo=None) if data_consultada else None
        }
        erros.append(erro)
    return erros

# Padroniza os erros de qualidade do estoque
def tratar_erros_qualidade_estoque(estoque, vendedor, data_consultada=None):
    erros = []
    for e in estoque:
        erro = {
            "asin": e.get("asin"),
            "disponivel_vendavel": "Sem estoque" if e.get("disponivel_vendavel") is None else "OK",
            "inutilizavel_total": "OK" if e.get("inutilizavel_total") is None or e.get("inutilizavel_total") == 0 else f"{e.get('inutilizavel_total')} itens inutilizáveis",
            "vendedor": vendedor,
            "data_registro": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_consultada": data_consultada.replace(tzinfo=None) if data_consultada else None
        }
        erros.append(erro)
    return erros

# ------------------------- SALVAR NO BANCO DE DADOS ----------------------------

def limpar_dados_antigos(vendedor):
    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco para limpar dados antigos.")
        return
    try:
        cursor = conn.cursor()
        tabelas = ["erros_qualidade_produtos", "erros_qualidade_estoque", "estoque", "pedidos", "faturamento", "produtos"]
        for tabela in tabelas:
            cursor.execute(f"DELETE FROM {tabela} WHERE vendedor = %s", (vendedor,))
        conn.commit()
        print(f"\nRegistros antigos removidos para o vendedor {vendedor}.")
    except Exception as e:
        print(f"Erro ao limpar dados antigos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def salvar_produtos_no_banco(produtos):
    if not produtos:
        return "Nenhum produto para salvar."
    vendedor = produtos[0].get("vendedor") if produtos else None
    if vendedor:
        limpar_dados_antigos(vendedor)
    conn = get_connection()
    if not conn:
        return "Erro ao conectar com o banco de dados."
    produtos_unicos = {}
    for p in produtos:
        chave = (p.get("asin"), p.get("vendedor"))
        produtos_unicos[chave] = p
    produtos_final = list(produtos_unicos.values())
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO produtos (asin, sku, tipo_produto, tipo_condicao, status, nome_item, data_criacao, data_atualizacao, imagem_url, imagem_largura, imagem_altura, vendedor, data_registro, data_consultada)
                VALUES %s
                ON CONFLICT (asin, vendedor) DO UPDATE SET
                    sku=EXCLUDED.sku,
                    tipo_produto=EXCLUDED.tipo_produto,
                    tipo_condicao=EXCLUDED.tipo_condicao,
                    status=EXCLUDED.status,
                    nome_item=EXCLUDED.nome_item,
                    data_criacao=EXCLUDED.data_criacao,
                    data_atualizacao=EXCLUDED.data_atualizacao,
                    imagem_url=EXCLUDED.imagem_url,
                    imagem_largura=EXCLUDED.imagem_largura,
                    imagem_altura=EXCLUDED.imagem_altura,
                    data_registro=EXCLUDED.data_registro,
                    data_consultada=EXCLUDED.data_consultada
            """, [(
                p.get("asin"),
                p.get("sku"),
                p.get("tipo_produto"),
                p.get("tipo_condicao"),
                p.get("status"),
                p.get("nome_item"),
                p.get("data_criacao"),
                p.get("data_atualizacao"),
                p.get("imagem_url"),
                p.get("imagem_largura"),
                p.get("imagem_altura"),
                p.get("vendedor"),
                p.get("data_registro"),
                p.get("data_consultada")
            ) for p in produtos_final])
        conn.commit()
        return f"{len(produtos_final)} produtos salvos com sucesso."
    except Exception as e:
        return f"Erro ao salvar produtos: {e}"
    finally:
        conn.close()

def salvar_pedidos_no_banco(pedidos):
    conn = get_connection()
    if not conn:
        return "Erro ao conectar com o banco de dados."
    if not pedidos:
        return "Nenhum pedido para salvar."
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO pedidos (id_pedido, municipio_comprador, status, data_compra, data_aprovacao, canal_venda, canal_fulfillment, detalhes_pagamento, total_pedido, moeda, itens_enviados, itens_nao_enviados, prime, pedido_empresarial, estado_entrega, cidade_entrega, vendedor, data_registro, data_consultada)
                VALUES %s
                ON CONFLICT (id_pedido, vendedor) DO UPDATE SET
                    municipio_comprador=EXCLUDED.municipio_comprador,
                    status=EXCLUDED.status,
                    data_compra=EXCLUDED.data_compra,
                    data_aprovacao=EXCLUDED.data_aprovacao,
                    canal_venda=EXCLUDED.canal_venda,
                    canal_fulfillment=EXCLUDED.canal_fulfillment,
                    detalhes_pagamento=EXCLUDED.detalhes_pagamento,
                    total_pedido=EXCLUDED.total_pedido,
                    moeda=EXCLUDED.moeda,
                    itens_enviados=EXCLUDED.itens_enviados,
                    itens_nao_enviados=EXCLUDED.itens_nao_enviados,
                    prime=EXCLUDED.prime,
                    pedido_empresarial=EXCLUDED.pedido_empresarial,
                    estado_entrega=EXCLUDED.estado_entrega,
                    cidade_entrega=EXCLUDED.cidade_entrega,
                    data_registro=EXCLUDED.data_registro,
                    data_consultada=EXCLUDED.data_consultada
            """, [(
                p.get("id_pedido"),
                p.get("municipio_comprador"),
                p.get("status"),
                p.get("data_compra"),
                p.get("data_aprovacao"),
                p.get("canal_venda"),
                p.get("canal_fulfillment"),
                p.get("detalhes_pagamento"),
                p.get("total_pedido"),
                p.get("moeda"),
                p.get("itens_enviados"),
                p.get("itens_nao_enviados"),
                p.get("prime"),
                p.get("pedido_empresarial"),
                p.get("estado_entrega"),
                p.get("cidade_entrega"),
                p.get("vendedor"),
                p.get("data_registro"),
                p.get("data_consultada")
            ) for p in pedidos])
        conn.commit()
        return f"{len(pedidos)} pedidos salvos com sucesso."
    except Exception as e:
        return f"Erro ao salvar pedidos: {e}"
    finally:
        conn.close()

def remover_duplicados_estoque(estoque):
    vistos = set()
    estoque_unico = []
    for e in estoque:
        chave = (e.get("asin"), e.get("vendedor"))
        if chave not in vistos:
            vistos.add(chave)
            estoque_unico.append(e)
    return estoque_unico

def salvar_estoque_no_banco(estoque):
    if not estoque:
        print("Nenhum estoque para salvar.")
        return "Nenhum estoque para salvar."
    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco de dados.")
        return "Erro ao conectar com o banco de dados."
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO estoque (asin, fnsku, condicao, disponivel_vendavel, recebendo_em_estoque, reservado_total, reservado_cliente, reservado_transito, reservado_processamento, em_pesquisa_total, pesquisa_curto_prazo, pesquisa_medio_prazo, pesquisa_longo_prazo, inutilizavel_total, inutilizavel_danificado_cliente, inutilizavel_danificado_armazem, inutilizavel_danificado_distribuidor, inutilizavel_danificado_transportadora, inutilizavel_defeituoso, inutilizavel_vencido, fornecimento_futuro_reservado, fornecimento_futuro_compravel, nome_produto, quantidade_total, ultima_atualizacao, vendedor, data_registro, data_consultada)
                VALUES %s
                ON CONFLICT (asin, vendedor) DO UPDATE SET
                    fnsku=EXCLUDED.fnsku,
                    condicao=EXCLUDED.condicao,
                    disponivel_vendavel=EXCLUDED.disponivel_vendavel,
                    recebendo_em_estoque=EXCLUDED.recebendo_em_estoque,
                    reservado_total=EXCLUDED.reservado_total,
                    reservado_cliente=EXCLUDED.reservado_cliente,
                    reservado_transito=EXCLUDED.reservado_transito,
                    reservado_processamento=EXCLUDED.reservado_processamento,
                    em_pesquisa_total=EXCLUDED.em_pesquisa_total,
                    pesquisa_curto_prazo=EXCLUDED.pesquisa_curto_prazo,
                    pesquisa_medio_prazo=EXCLUDED.pesquisa_medio_prazo,
                    pesquisa_longo_prazo=EXCLUDED.pesquisa_longo_prazo,
                    inutilizavel_total=EXCLUDED.inutilizavel_total,
                    inutilizavel_danificado_cliente=EXCLUDED.inutilizavel_danificado_cliente,
                    inutilizavel_danificado_armazem=EXCLUDED.inutilizavel_danificado_armazem,
                    inutilizavel_danificado_distribuidor=EXCLUDED.inutilizavel_danificado_distribuidor,
                    inutilizavel_danificado_transportadora=EXCLUDED.inutilizavel_danificado_transportadora,
                    inutilizavel_defeituoso=EXCLUDED.inutilizavel_defeituoso,
                    inutilizavel_vencido=EXCLUDED.inutilizavel_vencido,
                    fornecimento_futuro_reservado=EXCLUDED.fornecimento_futuro_reservado,
                    fornecimento_futuro_compravel=EXCLUDED.fornecimento_futuro_compravel,
                    nome_produto=EXCLUDED.nome_produto,
                    quantidade_total=EXCLUDED.quantidade_total,
                    ultima_atualizacao=EXCLUDED.ultima_atualizacao,
                    data_registro=EXCLUDED.data_registro,
                    data_consultada=EXCLUDED.data_consultada
            """, [(
                e.get("asin"),
                e.get("fnsku"),
                e.get("condicao"),
                e.get("disponivel_vendavel"),
                e.get("recebendo_em_estoque"),
                e.get("reservado_total"),
                e.get("reservado_cliente"),
                e.get("reservado_transito"),
                e.get("reservado_processamento"),
                e.get("em_pesquisa_total"),
                e.get("pesquisa_curto_prazo"),
                e.get("pesquisa_medio_prazo"),
                e.get("pesquisa_longo_prazo"),
                e.get("inutilizavel_total"),
                e.get("inutilizavel_danificado_cliente"),
                e.get("inutilizavel_danificado_armazem"),
                e.get("inutilizavel_danificado_distribuidor"),
                e.get("inutilizavel_danificado_transportadora"),
                e.get("inutilizavel_defeituoso"),
                e.get("inutilizavel_vencido"),
                e.get("fornecimento_futuro_reservado"),
                e.get("fornecimento_futuro_compravel"),
                e.get("nome_produto"),
                e.get("quantidade_total"),
                e.get("ultima_atualizacao"),
                e.get("vendedor"),
                e.get("data_registro"),
                e.get("data_consultada")
            ) for e in estoque])
        conn.commit()
    except Exception as e:
        print(f"Erro ao salvar estoque: {e}")
        return f"Erro ao salvar estoque: {e}"
    finally:
        conn.close()

def salvar_erros_qualidade_produtos(erros):
    if not erros:
        return "Nenhum erro de qualidade de produtos para salvar."
    erros_unicos = {}
    for e in erros:
        chave = (e.get("asin"), e.get("vendedor"))
        erros_unicos[chave] = e
    erros_final = list(erros_unicos.values())
    conn = get_connection()
    if not conn:
        return "Erro ao conectar com o banco de dados."
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO erros_qualidade_produtos (asin, sku, titulo, status, url_imagem_principal, resolucao_imagem, vendedor, data_registro, data_consultada)
                VALUES %s
                ON CONFLICT (asin, vendedor) DO UPDATE SET
                    sku=EXCLUDED.sku,
                    titulo=EXCLUDED.titulo,
                    status=EXCLUDED.status,
                    url_imagem_principal=EXCLUDED.url_imagem_principal,
                    resolucao_imagem=EXCLUDED.resolucao_imagem,
                    data_registro=EXCLUDED.data_registro,
                    data_consultada=EXCLUDED.data_consultada
            """, [(
                e.get("asin"),
                e.get("sku"),
                e.get("titulo"),
                e.get("status"),
                e.get("url_imagem_principal"),
                e.get("resolucao_imagem"),
                e.get("vendedor"),
                e.get("data_registro"),
                e.get("data_consultada")
            ) for e in erros_final])
        conn.commit()
        return f"{len(erros_final)} erros de qualidade de produtos salvos com sucesso."
    except Exception as e:
        return f"Erro ao salvar erros de qualidade de produtos: {e}"
    finally:
        conn.close()

def remover_duplicados_erros_estoque(erros):
    vistos = set()
    erros_unicos = []
    for e in erros:
        chave = (e.get("asin"), e.get("vendedor"))
        if chave not in vistos:
            vistos.add(chave)
            erros_unicos.append(e)
    return erros_unicos

def salvar_erros_qualidade_estoque(erros):
    if not erros:
        return "Nenhum erro de qualidade de estoque para salvar."
    erros = remover_duplicados_erros_estoque(erros)
    conn = get_connection()
    if not conn:
        return "Erro ao conectar com o banco de dados."
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO erros_qualidade_estoque (asin, disponivel_vendavel, inutilizavel_total, vendedor, data_registro, data_consultada)
                VALUES %s
                ON CONFLICT (asin, vendedor) DO UPDATE SET
                    disponivel_vendavel=EXCLUDED.disponivel_vendavel,
                    inutilizavel_total=EXCLUDED.inutilizavel_total,
                    data_registro=EXCLUDED.data_registro,
                    data_consultada=EXCLUDED.data_consultada
            """, [(
                e.get("asin"),
                e.get("disponivel_vendavel"),
                e.get("inutilizavel_total"),
                e.get("vendedor"),
                e.get("data_registro"),
                e.get("data_consultada")
            ) for e in erros])
        conn.commit()
        return f"{len(erros)} erros de qualidade de estoque salvos com sucesso."
    except Exception as e:
        return f"Erro ao salvar erros de qualidade de estoque: {e}"
    finally:
        conn.close()

def salvar_faturamento_no_banco(faturamento):
    conn = get_connection()
    if not conn:
        return "Erro ao conectar com o banco de dados."
    if not faturamento:
        return "Nenhum faturamento para salvar."
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO faturamento (
                    periodo_inicio, periodo_fim, unidades_vendidas, itens_vendidos, pedidos,
                    preco_medio_unitario, moeda_unitario, total_vendas, moeda_vendas,
                    vendedor, data_registro
                )
                VALUES %s
                ON CONFLICT (periodo_inicio, periodo_fim, vendedor) DO UPDATE SET
                    unidades_vendidas=EXCLUDED.unidades_vendidas,
                    itens_vendidos=EXCLUDED.itens_vendidos,
                    pedidos=EXCLUDED.pedidos,
                    preco_medio_unitario=EXCLUDED.preco_medio_unitario,
                    moeda_unitario=EXCLUDED.moeda_unitario,
                    total_vendas=EXCLUDED.total_vendas,
                    moeda_vendas=EXCLUDED.moeda_vendas,
                    data_registro=EXCLUDED.data_registro
            """, [(
                f.get("periodo_inicio"),
                f.get("periodo_fim"),
                f.get("unidades_vendidas"),
                f.get("itens_vendidos"),
                f.get("pedidos"),
                f.get("preco_medio_unitario"),
                f.get("moeda_unitario"),
                f.get("total_vendas"),
                f.get("moeda_vendas"),
                f.get("vendedor"),
                f.get("data_registro")
            ) for f in faturamento])
        conn.commit()
        return f"{len(faturamento)} registros de faturamento salvos com sucesso."
    except Exception as e:
        return f"Erro ao salvar faturamento: {e}"
    finally:
        conn.close()

# ------------------------- BUSCAR NO BANCO DE DADOS PARA DOWNLOAD ----------------------------

def buscar_produtos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM produtos WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

def buscar_pedidos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM pedidos WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

def buscar_estoque_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM estoque WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

def buscar_faturamento_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM faturamento WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

def buscar_erros_produtos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM erros_qualidade_produtos WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

def buscar_erros_estoque_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM erros_qualidade_estoque WHERE vendedor = %s AND data_registro::date = CURRENT_DATE
        """, (vendedor,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

# ------------------------- GERAR XLSX E ZIP ----------------------------

def df_to_xlsx_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def gerar_zip_relatorios_do_dia(vendedor):
    try:
        produtos = buscar_produtos_do_dia(vendedor)
        pedidos = buscar_pedidos_do_dia(vendedor)
        estoque = buscar_estoque_do_dia(vendedor)
        faturamento = buscar_faturamento_do_dia(vendedor)
        erros_produtos = buscar_erros_produtos_do_dia(vendedor)
        erros_estoque = buscar_erros_estoque_do_dia(vendedor)

        zip_stream = io.BytesIO()
        with zipfile.ZipFile(zip_stream, "w") as zf:
            if produtos is not None and not produtos.empty:
                zf.writestr("produtos.xlsx", df_to_xlsx_bytes(remover_timezone_df(produtos)))
            if pedidos is not None and not pedidos.empty:
                zf.writestr("pedidos.xlsx", df_to_xlsx_bytes(remover_timezone_df(pedidos)))
            if estoque is not None and not estoque.empty:
                zf.writestr("estoque_FBA.xlsx", df_to_xlsx_bytes(remover_timezone_df(estoque)))
            if faturamento is not None and not faturamento.empty:
                zf.writestr("faturamento.xlsx", df_to_xlsx_bytes(remover_timezone_df(faturamento)))
            if erros_produtos is not None and not erros_produtos.empty:
                zf.writestr("erros_qualidade_produtos.xlsx", df_to_xlsx_bytes(remover_timezone_df(erros_produtos)))
            if erros_estoque is not None and not erros_estoque.empty:
                zf.writestr("erros_qualidade_estoque_FBA.xlsx", df_to_xlsx_bytes(remover_timezone_df(erros_estoque)))
        zip_stream.seek(0)
        return zip_stream
    except Exception as e:
        print(f"Erro ao gerar ZIP Amazon: {e}")
        import traceback
        traceback.print_exc()
        raise

# ------------------------- EXECUÇÃO PRINCIPAL ----------------------------

# Função principal para coletar dados da Amazon
def coletar_dados_amazon(vendedor: str):
    print(f"\nIniciando coleta Amazon para o vendedor: {vendedor}")
    mensagens = []
    try:
        # Verifica se o vendedor está na lista de tokens
        tokens = load_tokens()
        if vendedor not in tokens:
            msg = f"Vendedor {vendedor} não encontrado nos tokens."
            print(msg)
            return msg
        
        # Obtém os tokens do vendedor
        refresh_token = tokens[vendedor]['refresh_token']
        seller_id = tokens[vendedor]['seller_id']
        access_token = get_access_token(refresh_token)
        if not access_token:
            msg = "Não foi possível obter access_token."
            print(msg)
            return msg

        # Produtos
        created_after_produtos = (datetime.now() - timedelta(days=730)).replace(tzinfo=timezone.utc)
        produtos_raw = get_listing_items(access_token, seller_id)
        produtos = tratar_dados_produtos(produtos_raw, vendedor, data_consultada=created_after_produtos)
        msg_produtos = salvar_produtos_no_banco(produtos)
        mensagens.append(msg_produtos)
        erros_produtos = tratar_erros_qualidade_produtos(produtos, vendedor, data_consultada=created_after_produtos)
        msg_erros_produtos = salvar_erros_qualidade_produtos(erros_produtos)
        mensagens.append(msg_erros_produtos)
        produtos_global = tratar_dados_produtos(produtos_raw, vendedor, data_consultada=created_after_produtos)
        produtos_dict = {(p['asin'], p['vendedor']): p['status'] for p in produtos_global}

        # Pedidos
        created_after_pedidos = (datetime.now(timezone.utc) - timedelta(days=30))
        pedidos_raw = get_orders(access_token)
        pedidos = tratar_dados_pedidos(pedidos_raw, vendedor, data_consultada=created_after_pedidos)
        msg_pedidos = salvar_pedidos_no_banco(pedidos)
        mensagens.append(msg_pedidos)

        # Estoque
        start_date_estoque = (datetime.now(timezone.utc) - timedelta(days=90))
        estoque_raw = get_fba_inventory_summaries(access_token)
        estoque = tratar_dados_estoque(estoque_raw, vendedor, data_consultada=start_date_estoque)
        msg_estoque = salvar_estoque_no_banco(estoque)
        mensagens.append(msg_estoque)
        erros_estoque = tratar_erros_qualidade_estoque(estoque, vendedor, data_consultada=start_date_estoque)
        msg_erros_estoque = salvar_erros_qualidade_estoque(erros_estoque)
        mensagens.append(msg_erros_estoque)

        # Faturamento
        faturamento_raw = get_order_metrics(access_token)
        faturamento = tratar_dados_faturamento(faturamento_raw, vendedor)
        msg_faturamento = salvar_faturamento_no_banco(faturamento)
        mensagens.append(msg_faturamento)

        print(f"\nColeta Amazon finalizada para {vendedor}\n")
        return f"\nColeta Amazon finalizada para {vendedor}"
    except Exception as e:
        print(f"Erro durante a coleta Amazon: {e}")
        import traceback
        traceback.print_exc()
        return f"Erro durante a coleta Amazon: {e}"

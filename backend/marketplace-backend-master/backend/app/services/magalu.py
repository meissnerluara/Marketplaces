import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import io
from datetime import datetime
import pytz
import zipfile

# ------------------------- VARIÁVEIS DE AMBIENTE ----------------------------

load_dotenv()
url_base_auth = os.getenv('MAGALU_URL_BASE_AUTH')
url_base_api = os.getenv('MAGALU_URL_BASE_API')
client_id = os.getenv('MAGALU_CLIENT_ID')
client_secret = os.getenv('MAGALU_CLIENT_SECRET')

# ------------------------- CONFIGURAÇÃO BANCO DE DADOS ----------------------------

# Conexão com o banco de dados PostgreSQL
def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("MAGALU_DB_HOST"),
            port=os.getenv("MAGALU_DB_PORT"),
            dbname=os.getenv("MAGALU_DB_NAME"),
            user=os.getenv("MAGALU_DB_USER"),
            password=os.getenv("MAGALU_DB_PASSWORD"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        print("\nErro ao conectar com o banco de dados no Supabase:", e)
        return None

# ------------------------- TOKENS ----------------------------

# Carrega os tokens do ambiente
def load_tokens():
    tokens_env = os.getenv("MAGALU_TOKENS")
    if tokens_env:
        try:
            data = json.loads(tokens_env)
            if not isinstance(data, dict):
                return {}
            return data
        except Exception:
            return {}
    return {}

# Renova o access_token usando o refresh_token
def refresh_access_token(client_id, client_secret, refresh_token):
    url = f"{url_base_auth}/oauth/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        data = response.json()
        new_access_token = data['access_token']
        new_refresh_token = data.get('refresh_token', refresh_token)
        print("Token renovado com sucesso!")
        return new_access_token, new_refresh_token
    else:
        print(f"\nErro ao renovar token: Status {response.status_code}")
        raise Exception()

# ------------------------- CHAMADAS API ----------------------------

# Fazer requisições à API da Magalu
def make_request(url, headers, params=None, refresh_token_func=None):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            return response
        else:
            print(f"\nRequisição falhou — {response.status_code}")

        if response.status_code == 401 and refresh_token_func:
            print("Token expirado. Tentando renovar...")
            headers = refresh_token_func()
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response
            else:
                print(f"Após renovação, ainda falhou — {response.status_code}")

        return None

    except requests.exceptions.RequestException as e:
        print("Erro na requisição:", e)
        return None

# Listar todos os SKUs de um vendedor
def listar_todos_skus(headers, refresh_token_func=None, limit=100):
    todos_skus = []
    offset = 0

    while True:
        params = {'_limit': limit, '_offset': offset}
        url = f"{url_base_api}/seller/v1/portfolios/skus"
        response = make_request(url, headers, params=params, refresh_token_func=refresh_token_func)

        if response is None or response.status_code != 200:
            print(f"Erro ao listar SKUs: Status {response.status_code}" if response else "Sem resposta")
            print(f"Resposta: Status {response.status_code}" if response else "None")
            break

        dados = response.json()
        resultados = dados.get("results", [])
        if not resultados:
            break

        todos_skus.extend(resultados)

        if len(resultados) < limit:
            break

        offset += limit

    return {"results": todos_skus}

# Consultar informações de um SKU específico
def consultar_sku(headers, sku_id, refresh_token_func=None):
    url = f"{url_base_api}/seller/v1/portfolios/skus/{sku_id}"
    response = make_request(url, headers, refresh_token_func=refresh_token_func)

    if response and response.status_code == 200:
        return response.json()
    print(f"Erro ao consultar SKU {sku_id} — status {response.status_code}")
    return None

# Consultar preço de um SKU
def consultar_preco(headers, sku_id, refresh_token_func=None):
    url = f"{url_base_api}/seller/v1/portfolios/prices/{sku_id}?_limit=100"
    response = make_request(url, headers, refresh_token_func=refresh_token_func)

    if response and response.status_code == 200:
        return response.json()
    print(f"Erro ao consultar preço do SKU {sku_id} — status {response.status_code}")
    return None

# Consultar estoque de um SKU
def consultar_estoque(headers, sku_id, refresh_token_func=None):
    url = f"{url_base_api}/seller/v1/portfolios/stocks/{sku_id}?_limit=100"
    response = make_request(url, headers, refresh_token_func=refresh_token_func)

    if response is None:
        print(f"Erro ao consultar estoque do SKU {sku_id} — resposta None")
        return None
    if response and response.status_code == 200:
        return response.json()
    print(f"Erro ao consultar estoque do SKU {sku_id} — status {response.status_code}")
    return None

# Listar pedidos de um vendedor
def listar_pedidos(headers, refresh_token_func=None, limit=100):
    todos_pedidos = []
    offset = 0

    while True:
        params = {'_limit': limit, '_offset': offset}
        url = f"{url_base_api}/seller/v1/orders"
        response = make_request(url, headers, params=params, refresh_token_func=refresh_token_func)

        if response is None or response.status_code != 200:
            break

        dados = response.json()
        pedidos_lote = dados.get("results", [])
        if not pedidos_lote:
            break

        todos_pedidos.extend(pedidos_lote)

        if len(pedidos_lote) < limit:
            break

        offset += limit

    print(f"Total de pedidos coletados: {len(todos_pedidos)}")
    return {"results": todos_pedidos}

# ------------------------- OBTENÇÃO DE DADOS ----------------------------

# Caso os dados dos produtos sejam obtidos de vários endpoints, eles devem ser combinados aqui.

# Obtém todos os dados de produtos de um vendedor
def obter_todos_os_dados(dados_skus, access_token, refresh_token, nickname):
    token_data = {'access_token': access_token, 'refresh_token': refresh_token}
    headers = {'Authorization': f'Bearer {token_data["access_token"]}'}

    def refresh_token_func():
        new_access_token, new_refresh_token = refresh_access_token(
            client_id, client_secret, token_data['refresh_token']
        )
        token_data['access_token'] = new_access_token
        token_data['refresh_token'] = new_refresh_token
        headers['Authorization'] = f'Bearer {new_access_token}'
        return headers

    produtos = []
    atributos = []
    imagens = []

    print(f"\nToken validado!")
    print(f"\nTotal de SKUs coletados: {len(dados_skus.get('results', []))}")

    for item in dados_skus.get("results", []):
        sku_id = item.get("sku")
        if not sku_id:
            print("SKU sem ID encontrado, pulando este item.")
            continue

        preco = consultar_preco(headers, sku_id, refresh_token_func)
        estoque = consultar_estoque(headers, sku_id, refresh_token_func)
        info = consultar_sku(headers, sku_id, refresh_token_func)

        preco_info = preco.get("results", [{}])[0] if preco and "results" in preco else {}
        estoque_info = estoque.get("results", [{}])[0] if estoque and "results" in estoque else {}

        # Tradução do status
        status_map = {
            "INACTIVE": "Inativo",
            "UNPUBLISHED": "Não publicado",
            "PUBLISHED": "Publicado",
            "BLOCKED": "Bloqueado"
        }
        status_original = info.get("status", "") if info else ""
        status_traduzido = status_map.get(status_original.upper(), status_original)

        # PRODUTOS
        produto = {
            "sku_id": sku_id,
            "titulo": info.get("title", "") if info else "",
            "descricao": info.get("description", "") if info else "",
            "marca": info.get("brand", "") if info else "",
            "status": status_traduzido,
            "data_criacao": info.get("created_at") if info else None,
            "data_atualizacao": info.get("updated_at") if info else None,
            "preco": round(preco_info.get("price", 0) / 100, 2),
            "estoque_disponivel": estoque_info.get("quantity", 0)
        }
        produtos.append(produto)

        # ATRIBUTOS
        for attr in item.get("attributes", []):
            nome = attr.get("name", "")
            valor = attr.get("value", "")
            # Tradução dos nomes dos atributos
            if nome == "update_only_front":
                nome_traduzido = "Apenas atualização no frontend"
            elif nome == "color":
                nome_traduzido = "Cor"
            else:
                nome_traduzido = nome
            if nome and valor and nome != "IdProduct" and nome != "fulfillment":
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": nome_traduzido,
                    "valor": valor
                })

        # DATASHEET
        for attr in info.get("datasheet", []) if info else []:
            nome = attr.get("name", "")
            valor = attr.get("value", "")
            if nome == "update_only_front":
                nome_traduzido = "Apenas atualização no frontend"
            elif nome == "color":
                nome_traduzido = "Cor"
            else:
                nome_traduzido = nome
            if nome and valor and nome != "IdProduct" and nome != "fulfillment":
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": nome_traduzido,
                    "valor": valor
                })

        # EXTRA_DATA
        for attr in info.get("extra_data", []) if info else []:
            nome = attr.get("name", "")
            valor = attr.get("value", "")
            if nome == "update_only_front":
                nome_traduzido = "Apenas atualização no frontend"
            elif nome == "color":
                nome_traduzido = "Cor"
            else:
                nome_traduzido = nome
            if nome and valor and nome != "IdProduct" and nome != "fulfillment":
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": nome_traduzido,
                    "valor": valor
                })

        # DIMENSIONS
        dim = info.get("dimensions", {})
        if isinstance(dim, dict):
            if dim.get("height", {}).get("value"):
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": "Altura (cm)",
                    "valor": dim["height"]["value"]
                })
            if dim.get("width", {}).get("value"):
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": "Largura (cm)",
                    "valor": dim["width"]["value"]
                })
            if dim.get("length", {}).get("value"):
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": "Comprimento (cm)",
                    "valor": dim["length"]["value"]
                })
            if dim.get("weight", {}).get("value"):
                atributos.append({
                    "sku_id": sku_id,
                    "atributo": "Peso (g)",
                    "valor": dim["weight"]["value"]
                })

        # IMAGENS
        for idx, img in enumerate(info.get("images", []) if info else []):
            imagens.append({
                "id_imagem": f"{sku_id}_{idx}",
                "sku_id": sku_id,
                "secure_url": img.get("reference"),
                "resolucao": img.get("type")
            })

    return produtos, atributos, imagens

# Processa os pedidos obtidos da API Magalu
def processar_pedidos(pedidos_raw):
    pedidos = []
    lista = pedidos_raw.get("results", [])

    # Dicionários de tradução
    status_map = {
        "created": "Criado",
        "finished": "Finalizado",
        "cancelled": "Cancelado"
    }

    pagamento_map = {
        "created": "Criado",
        "finished": "Pago",
        "cancelled": "Cancelado"
    }

    metodo_map = {
        "credit_card": "Cartão de crédito",
        "bank_slip": "Boleto bancário"
    }

    for item in lista:
        amounts = item.get("amounts", {})
        payments = item.get("payments", [])

        total_raw = amounts.get("total", 0)
        normalizer = amounts.get("normalizer", 100) or 100
        valor = total_raw / normalizer

        raw_status = item.get("status", "")
        status = status_map.get(raw_status, raw_status)
        pagamento_status = pagamento_map.get(raw_status, raw_status)

        method = payments[0].get("method") if payments else ""
        metodo_pagamento = metodo_map.get(method, method)
        currency = payments[0].get("currency") if payments else ""

        pedidos.append({
            "id": item.get("id"),
            "status": status,
            "data_criacao": item.get("created_at"),
            "valor": valor,
            "pagamento_status": pagamento_status,
            "metodo_pagamento": metodo_pagamento,
            "moeda": currency
        })

    return pedidos

# ------------------------- TRATAMENTO DE DADOS ----------------------------

# Trata os dados verificando erros comuns e salvando um relatório de erros
def tratar_dados(df_produtos, df_imagens, df_atributos):
    def contar_imagens_baixa_resolucao(resolucoes):
        baixa = 0
        for r in resolucoes:
            try:
                w, h = map(int, r.lower().split('x'))
                if w < 1000 or h < 1000:
                    baixa += 1
            except:
                continue
        return baixa

    erros = []

    for _, row in df_produtos.iterrows():
        sku = row['sku_id']
        titulo = row['titulo']
        descricao = row['descricao']
        status = row.get('status', '')

        imagens_produto = df_imagens[df_imagens['sku_id'] == sku] if df_imagens is not None else pd.DataFrame()
        qtd_imagens = len(imagens_produto)
        qtd_imagem_msg = "OK" if qtd_imagens > 3 else f"Necessário adicionar mais {3 - qtd_imagens} imagens"
        resolucoes = imagens_produto['resolucao'].dropna().tolist() if 'resolucao' in imagens_produto else []
        baixa_qtd = contar_imagens_baixa_resolucao(resolucoes)
        resolucao_msg = "OK" if baixa_qtd == 0 else f"{baixa_qtd} imagens com qualidade baixa"
        descricao_msg = "OK" if pd.notna(descricao) and str(descricao).strip() != "" and len(str(descricao)) > 500 else "Necessário preencher"
        titulo_msg = "OK" if pd.notna(titulo) and 10 <= len(str(titulo)) <= 60 else "Necessário preencher"
        marca_msg = "OK" if pd.notna(row['marca']) and row['marca'].strip() != "" else "Necessário preencher"
        atributos_produto = df_atributos[df_atributos['sku_id'] == sku] if df_atributos is not None else pd.DataFrame()
        atributos_vazios = atributos_produto['valor'].isna().sum() + (atributos_produto['valor'] == '').sum() if 'valor' in atributos_produto else 0
        atributos_msg = f"{atributos_vazios} campos vazios"

        erros.append({
            'sku_id': sku,
            'produto': titulo,
            'status': status,
            'titulo': titulo_msg,
            'qtd_imagem': qtd_imagem_msg,
            'resolucao_imagem': resolucao_msg,
            'descricao': descricao_msg,
            'atributos': atributos_msg,
            'marca': marca_msg
        })

    df_erros = pd.DataFrame(erros)
    return df_erros

# ------------------------- SALVAR NO BANCO DE DADOS ----------------------------

# Limpa dados antigos do vendedor
def limpar_dados_antigos(vendedor):
    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco para limpar dados antigos.")
        return
    try:
        cursor = conn.cursor()
        tabelas = ["erros_qualidade", "atributos", "imagens", "pedidos", "produtos"]
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

# Salva os dados no banco de dados
def salvar_no_banco(produtos, atributos, imagens, pedidos, vendedor):

    limpar_dados_antigos(vendedor)

    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco de dados no Supabase.")
        return

    try:
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        batch_size = 500
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_registro = datetime.now(fuso_brasilia).replace(tzinfo=None)

        # PRODUTOS
        produtos_valores = [
            (
                p['sku_id'],
                p['titulo'],
                p['descricao'],
                p['marca'],
                p['status'],
                p['preco'],
                p['estoque_disponivel'],
                p['data_criacao'],
                p['data_atualizacao'],
                vendedor,
                data_registro
            )
            for p in produtos
        ]

        query_produtos = """
            INSERT INTO produtos (
                sku_id, titulo, descricao, marca, status, preco, estoque_disponivel,
                data_criacao, data_atualizacao, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (sku_id, vendedor)
            DO UPDATE SET
                titulo = EXCLUDED.titulo,
                descricao = EXCLUDED.descricao,
                marca = EXCLUDED.marca,
                status = EXCLUDED.status,
                preco = EXCLUDED.preco,
                estoque_disponivel = EXCLUDED.estoque_disponivel,
                data_criacao = EXCLUDED.data_criacao,
                data_atualizacao = EXCLUDED.data_atualizacao,
                data_registro = EXCLUDED.data_registro;
        """

        template_produtos = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for i in range(0, len(produtos_valores), batch_size):
            try:
                execute_values(
                    cursor,
                    query_produtos,
                    produtos_valores[i:i+batch_size],
                    template=template_produtos,
                    page_size=batch_size
                )
            except Exception as e:
                print(f"Erro ao inserir batch de produtos ({i}): {e}")

        # IMAGENS
        imagens_valores = [
            (img['id_imagem'], img['sku_id'], img['secure_url'], img['resolucao'], vendedor, data_registro)
            for img in imagens
        ]

        query_imagens = """
            INSERT INTO imagens (
                id_imagem, sku_id, secure_url, resolucao, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (id_imagem, sku_id, vendedor)
            DO UPDATE SET
                secure_url = EXCLUDED.secure_url,
                resolucao = EXCLUDED.resolucao,
                data_registro = EXCLUDED.data_registro;
        """

        template_imagens = "(%s, %s, %s, %s, %s, %s)"
        for i in range(0, len(imagens_valores), batch_size):
            try:
                execute_values(
                    cursor,
                    query_imagens,
                    imagens_valores[i:i+batch_size],
                    template=template_imagens,
                    page_size=batch_size
                )
            except Exception as e:
                print(f"Erro ao inserir batch de imagens ({i}): {e}")

        # ATRIBUTOS
        atributos_validos = []
        for idx, a in enumerate(atributos):
            try:
                if not isinstance(a, dict):
                    print(f"Atributo malformado (não é dict): {a}")
                    continue
                if a.get('atributo') and a.get('valor') is not None:
                    atributos_validos.append(a)
            except Exception as e:
                print(f"Erro no atributo #{idx}: {a} — {e}")

        atributos_valores = [
            (a['sku_id'], a['atributo'], a['valor'], vendedor, data_registro) for a in atributos_validos
        ]

        query_atributos = """
            INSERT INTO atributos (
                sku_id, atributo, valor, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (sku_id, atributo, vendedor)
            DO UPDATE SET
                valor = EXCLUDED.valor,
                data_registro = EXCLUDED.data_registro;
        """

        template_atributos = "(%s, %s, %s, %s, %s)"
        for i in range(0, len(atributos_valores), batch_size):
            try:
                execute_values(
                    cursor,
                    query_atributos,
                    atributos_valores[i:i+batch_size],
                    template=template_atributos,
                    page_size=batch_size
                )
            except Exception as e:
                print(f"Erro ao inserir batch de atributos ({i}): {e}")


        # PEDIDOS
        pedidos_valores = [
            (
                p['id'],
                p['status'],
                p['data_criacao'],
                p['valor'],
                p['pagamento_status'],
                p['metodo_pagamento'], 
                p['moeda'], 
                vendedor,
                data_registro
            )
            for p in pedidos
        ]

        query_pedidos = """
            INSERT INTO pedidos (
                id, status, data_criacao, valor, pagamento_status,
                metodo_pagamento, moeda, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (id)
            DO UPDATE SET
                status = EXCLUDED.status,
                data_criacao = EXCLUDED.data_criacao,
                valor = EXCLUDED.valor,
                pagamento_status = EXCLUDED.pagamento_status,
                metodo_pagamento = EXCLUDED.metodo_pagamento,
                moeda = EXCLUDED.moeda,
                vendedor = EXCLUDED.vendedor,
                data_registro = EXCLUDED.data_registro;
        """

        template_pedidos = "(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for i in range(0, len(pedidos_valores), batch_size):
            try:
                execute_values(
                    cursor,
                    query_pedidos,
                    pedidos_valores[i:i+batch_size],
                    template=template_pedidos,
                    page_size=batch_size
                )
            except Exception as e:
                print(f"Erro ao inserir batch de pedidos ({i}): {e}")


        conn.commit()
        print("Dados salvos no banco de dados.")

    except Exception as e:
        conn.rollback()
        print(f"\nErro ao salvar no banco de dados: {e}")
    finally:
        cursor.close()
        conn.close()

# Salva os erros no banco de dados
def salvar_erros_no_banco(df_erros, vendedor):
    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco para salvar erros.")
        return

    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_registro = datetime.now(fuso_brasilia).replace(tzinfo=None)

        # Filtra apenas erros cujo sku_id existe em produtos
        cursor.execute(
            "SELECT sku_id FROM produtos WHERE vendedor = %s", (vendedor,)
        )
        skus_validos = set(row[0] for row in cursor.fetchall())
        df_erros = df_erros[df_erros['sku_id'].isin(skus_validos)]

        valores = [
            (
                row['sku_id'],
                row['produto'],
                row['status'],
                row['titulo'],
                row['qtd_imagem'],
                row['resolucao_imagem'],
                row['descricao'],
                row['atributos'],
                row['marca'],
                vendedor,
                data_registro
            )
            for _, row in df_erros.iterrows()
        ]

        query = """
            INSERT INTO erros_qualidade (
                sku_id, produto, status, titulo, qtd_imagem, resolucao_imagem,
                descricao, atributos, marca, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (sku_id, vendedor)
            DO UPDATE SET
                produto = EXCLUDED.produto,
                status = EXCLUDED.status,
                titulo = EXCLUDED.titulo,
                qtd_imagem = EXCLUDED.qtd_imagem,
                resolucao_imagem = EXCLUDED.resolucao_imagem,
                descricao = EXCLUDED.descricao,
                atributos = EXCLUDED.atributos,
                marca = EXCLUDED.marca,
                data_registro = EXCLUDED.data_registro;
        """

        template_erros = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        execute_values(
            cursor,
            query,
            valores,
            template=template_erros
        )

        conn.commit()

    except Exception as e:
        print("Erro ao salvar erros no banco:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ------------------------- BUSCAR NO BANCO DE DADOS PARA DOWNLOAD ----------------------------

# Busca os dados do dia para download
def buscar_produtos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM produtos WHERE vendedor = %s AND data_registro::date = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# Busca os atributos do dia para download
def buscar_atributos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM atributos WHERE vendedor = %s AND data_registro::date = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# Busca as imagens do dia para download
def buscar_imagens_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM imagens WHERE vendedor = %s AND data_registro::date = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# Busca os pedidos do dia para download
def buscar_pedidos_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM pedidos WHERE vendedor = %s AND data_registro::date = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# Busca os erros do dia para download
def buscar_erros_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM erros_qualidade WHERE vendedor = %s AND data_registro::date = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(zip(colunas, row)) for row in rows])
    finally:
        cursor.close()
        conn.close()

# ------------------------- GERAR XLSX E ZIP----------------------------

# Gera um arquivo ZIP com os relatórios do dia
def gerar_zip_relatorios_do_dia(vendedor):

    produtos = buscar_produtos_do_dia(vendedor)
    atributos = buscar_atributos_do_dia(vendedor)
    imagens = buscar_imagens_do_dia(vendedor)
    pedidos = buscar_pedidos_do_dia(vendedor)
    df_erros = buscar_erros_do_dia(vendedor)

    def df_to_xlsx_bytes(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return output.read()

    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w") as zf:
        if produtos:
            df_produtos = pd.DataFrame(produtos)
            zf.writestr(f"produtos_{vendedor}.xlsx", df_to_xlsx_bytes(df_produtos))
        if imagens:
            df_imagens = pd.DataFrame(imagens)
            zf.writestr(f"imagens_{vendedor}.xlsx", df_to_xlsx_bytes(df_imagens))
        if atributos:
            df_atributos = pd.DataFrame(atributos)
            zf.writestr(f"atributos_{vendedor}.xlsx", df_to_xlsx_bytes(df_atributos))
        if pedidos:
            df_pedidos = pd.DataFrame(pedidos)
            zf.writestr(f"pedidos_{vendedor}.xlsx", df_to_xlsx_bytes(df_pedidos))
        if df_erros is not None and not df_erros.empty:
            zf.writestr(f"erros_gerais_{vendedor}.xlsx", df_to_xlsx_bytes(df_erros))
    zip_stream.seek(0)
    return zip_stream

# -------------------------------- EXECUÇÃO PRINCIPAL --------------------------------

# Função principal para coletar dados da Magalu
def coletar_dados_magalu(vendedor: str):

    print(f"\nIniciando coleta Magalu para o vendedor: {vendedor}")

    # Verifica se o vendedor está na lista de tokens
    tokens = load_tokens()
    if vendedor not in tokens:
        raise Exception(f"Vendedor '{vendedor}' não encontrado.")

    # Obtém os tokens do vendedor
    seller_data = tokens[vendedor]
    access_token = seller_data['access_token']
    refresh_token = seller_data['refresh_token']
    token_data = {'access_token': access_token, 'refresh_token': refresh_token}
    headers = {'Authorization': f'Bearer {token_data["access_token"]}'}

    # Verifica se os tokens são válidos
    def refresh_token_func():
        new_access_token, new_refresh_token = refresh_access_token(
            client_id, client_secret, token_data['refresh_token']
        )
        token_data['access_token'] = new_access_token
        token_data['refresh_token'] = new_refresh_token
        headers['Authorization'] = f'Bearer {new_access_token}'
        return headers

    # Coleta dados de SKUs
    dados_skus = listar_todos_skus(headers, refresh_token_func=refresh_token_func)
    if not dados_skus or not dados_skus.get("results"):
        raise Exception("Falha ao acessar SKUs, mesmo após renovação de token.")

    # Coleta dados detalhados
    produtos, atributos, imagens = obter_todos_os_dados(
        dados_skus, token_data['access_token'], token_data['refresh_token'], vendedor
    )

    # Coleta pedidos
    pedidos_raw = listar_pedidos(headers, refresh_token_func=refresh_token_func)
    pedidos = processar_pedidos(pedidos_raw) if pedidos_raw else []

    # Salva no banco
    salvar_no_banco(produtos, atributos, imagens, pedidos, vendedor)

    # Gera erros de qualidade e salva no banco
    df_produtos = pd.DataFrame(produtos)
    df_imagens = pd.DataFrame(imagens)
    df_atributos = pd.DataFrame(atributos)
    df_erros = tratar_dados(df_produtos, df_imagens, df_atributos)
    salvar_erros_no_banco(df_erros, vendedor)

    print(f"\nColeta Magalu finalizada para {vendedor}")
    return f"\nColeta Magalu finalizada para {vendedor}"

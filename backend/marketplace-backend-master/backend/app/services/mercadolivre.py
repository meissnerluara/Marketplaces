import requests
import time
import json
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import io
import zipfile
import pytz

# ------------------------- VARIÁVEIS DE AMBIENTE ----------------------------

load_dotenv()
url_base = os.getenv('MERCADOLIVRE_URL_BASE')
client_id = os.getenv('MERCADOLIVRE_CLIENT_ID')
client_secret = os.getenv('MERCADOLIVRE_CLIENT_SECRET')

# ------------------------- CONFIGURAÇÃO BANCO DE DADOS ----------------------------

# Conexão com o banco de dados PostgreSQL
def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("MERCADOLIVRE_DB_HOST"),
            port=os.getenv("MERCADOLIVRE_DB_PORT"),
            dbname=os.getenv("MERCADOLIVRE_DB_NAME"),
            user=os.getenv("MERCADOLIVRE_DB_USER"),
            password=os.getenv("MERCADOLIVRE_DB_PASSWORD"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        print("\nErro ao conectar com o banco de dados no Supabase:", e)
        return None

# ------------------------- TOKENS ----------------------------

# Carrega os tokens do ambiente
def load_tokens():
    tokens_env = os.getenv("MERCADOLIVRE_TOKENS")
    if tokens_env:
        try:
            data = json.loads(tokens_env)
            if not isinstance(data, dict):
                return {}
            return data
        except Exception:
            return {}
    return {}

# Obtém o nickname do vendedor usando o seller_id e access_token
def get_nickname(seller_id, access_token):
    url = f"{url_base}/users/{seller_id}"
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('nickname')
    else:
        raise Exception("Erro ao obter o nickname do vendedor")

# Renova o access_token usando o refresh_token
def refresh_access_token(client_id, client_secret, refresh_token, nickname, seller_id):
    url = f"{url_base}/oauth/token"
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

# Fazer requisições à API do Mercado Livre
def make_request(url, headers, params=None, refresh_token_func=None):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response
        else:
            print(f"\nRequisição falhou — status {response.status_code}")

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

# Obtém todos os IDs de produtos do vendedor
def get_all_product_ids(seller_id, headers, refresh_token_func):
    url = f"{url_base}/users/{seller_id}/items/search"
    all_ids = []
    scroll_id = None
    params = {'search_type': 'scan'}
    while True:
        if scroll_id:
            params['scroll_id'] = scroll_id
        response = make_request(url, params=params, headers=headers, refresh_token_func=refresh_token_func)
        if response is None or response.status_code != 200:
            print(f"Erro ao obter IDs de produtos: status {response.status_code if response else 'sem resposta'}")
            break
        data = response.json()
        produtos = data.get("results", [])
        scroll_id = data.get("scroll_id")
        if not produtos:
            break
        all_ids.extend(produtos)
        time.sleep(0.5)
        if not scroll_id:
            break
    return all_ids

# Obtém detalhes do produto usando o item_id
def get_product_details(item_id, headers, refresh_token_func):
    url = f"{url_base}/items/{item_id}"
    response = make_request(url, headers=headers, refresh_token_func=refresh_token_func)
    if response and response.status_code == 200:
        return response.json()
    else:
        return None

# Obtém a descrição do produto usando o item_id
def get_product_description(item_id, headers, refresh_token_func):
    url = f'{url_base}/items/{item_id}/description'
    response = make_request(url, headers=headers, refresh_token_func=refresh_token_func)
    if response and response.status_code == 200:
        description = response.json()
        return description.get('plain_text', "")
    return "Erro de conexão"

# Obtém o nome da categoria do produto usando o category_id
def buscar_categoria_produto(category_id, headers, refresh_token_func):
    url = f"{url_base}/categories/{category_id}"
    response = make_request(url, headers=headers, refresh_token_func=refresh_token_func)
    if response and response.status_code == 200:
        dados = response.json()
        return dados.get('name', 'Categoria não encontrada')
    return 'Erro ao buscar categoria'

# ------------------------- OBTENÇÃO DE DADOS ----------------------------

# Caso os dados dos produtos sejam obtidos de vários endpoints, eles devem ser combinados aqui.

# Obtém todos os dados de produtos de um vendedor
def obter_todos_os_dados(seller_id, access_token, refresh_token, nickname):
    time.sleep(0.5)
    token_data = {'access_token': access_token, 'refresh_token': refresh_token}
    headers = {'Authorization': f'Bearer {token_data["access_token"]}'}

    def refresh_token_func():
        new_access_token, new_refresh_token = refresh_access_token(
            client_id, client_secret, token_data['refresh_token'], nickname, seller_id
        )
        token_data['access_token'] = new_access_token
        token_data['refresh_token'] = new_refresh_token
        headers['Authorization'] = f'Bearer {new_access_token}'
        return headers

    produtos_ids = get_all_product_ids(seller_id, headers, refresh_token_func)

    produtos = []
    imagens = []
    atributos = []
    variacoes = []

    print(f"\nToken validado!")
    print(f"\nTotal de SKUs coletados: {len(produtos_ids)}\n")

    for item_id in produtos_ids:
        detalhes = get_product_details(item_id, headers, refresh_token_func)
        if not detalhes:
            continue

        descricao = get_product_description(item_id, headers, refresh_token_func)
        descricao_tratada = tratar_descricao(descricao)
        nome_categoria = buscar_categoria_produto(detalhes.get('category_id'), headers, refresh_token_func)
        imagens_item = detalhes.get('pictures', [])
        atributos_item = detalhes.get('attributes', [])
        variacoes_item = detalhes.get('variations', [])

        # Garantia
        garantia_valor = detalhes.get('warranty')
        if garantia_valor is None or str(garantia_valor).lower() == "null":
            garantia_valor = "Sem garantia informada"

        # PRODUTOS
        produto = {
            "sku_id": detalhes.get('id'),
            "titulo": detalhes.get('title', ''),
            "descricao": descricao_tratada,
            "categoria_id": detalhes.get('category_id'),
            "nome_categoria": nome_categoria,
            "preco": detalhes.get('price', 0),
            "quantidade_variacoes": len(variacoes_item),
            "status": traduzir_status(detalhes.get('status', '')),
            "health": detalhes.get('health', ''),
            "quantidade_inicial": detalhes.get('initial_quantity', 0),
            "quantidade_vendida": detalhes.get('sold_quantity', 0),
            "quantidade_disponivel": detalhes.get('available_quantity', 0),
            "gtin": next((a.get('value_name') for a in atributos_item if a.get('id') == 'GTIN'), ''),
            "marca": next((a.get('value_name') for a in atributos_item if a.get('id') == 'BRAND'), ''),
            "permalink": detalhes.get('permalink', ''),
            "aceita_mercado_pago": detalhes.get('accepts_mercadopago', False),
            "garantia": garantia_valor,
            "imagens": len(imagens_item),
            "link_imagem": ', '.join([img.get('secure_url') for img in imagens_item if img.get('secure_url')])
        }
        produtos.append(produto)

        # ATRIBUTOS
        for attr in atributos_item:
            nome = attr.get("name", "")
            valor = attr.get("value_name", "")
            if nome and valor and nome != "IdProduct":
                atributos.append({
                    "sku_id": detalhes.get('id'),
                    "atributo": nome,
                    "valor": valor
                })

        # IMAGENS
        for img in imagens_item:
            imagens.append({
                "id_imagem": img.get("id"),
                "sku_id": detalhes.get('id'),
                "secure_url": img.get("secure_url"),
                "resolucao": img.get("size")
            })

        # VARIAÇÕES
        for variacao in variacoes_item:
            id_variacao = variacao.get('id')
            preco_variacao = variacao.get('price')
            atributos_var = variacao.get('attribute_combinations', [])
            for atributo in atributos_var:
                variacoes.append({
                    'sku_id': detalhes.get('id'),
                    'id_variacao': id_variacao,
                    'preco_variacao': preco_variacao,
                    'atributo': atributo.get('name'),
                    'valor': atributo.get('value_name')
                })

    return produtos, imagens, atributos, variacoes

# ------------------------- TRATAMENTO DE DADOS ----------------------------

# Trata a descrição do produto
def tratar_descricao(descricao):
    if pd.isna(descricao) or str(descricao).strip() == "":
        return 'Sem descrição'
    return descricao

# Traduz o status do produto
def traduzir_status(status):
    if status == "closed":
        return "Fechado"
    elif status == "active":
        return "Ativo"
    elif status == "paused":
        return "Pausado"
    return status

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
        sku_id = row['sku_id']
        titulo = row['titulo']
        descricao = row['descricao']
        status = row.get('status', '')

        descricao_tratada = tratar_descricao(descricao)
        status_traduzido = traduzir_status(status)

        garantia = row.get('garantia', '')
        if garantia is None or str(garantia).strip().lower() in ["", "null", "sem garantia informada"]:
            garantia_erro = "Sem garantia informada"
        else:
            garantia_erro = "OK"

        imagens_produto = df_imagens[df_imagens['sku_id'] == sku_id] if df_imagens is not None else pd.DataFrame()
        qtd_imagens = len(imagens_produto)
        qtd_imagem_msg = "OK" if qtd_imagens >= 6 else f"Necessário adicionar mais {6 - qtd_imagens} imagens"

        resolucoes = imagens_produto['resolucao'].dropna().tolist() if 'resolucao' in imagens_produto else []
        baixa_qtd = contar_imagens_baixa_resolucao(resolucoes)
        resolucao_msg = "OK" if baixa_qtd == 0 else f"{baixa_qtd} imagens com a qualidade baixa"

        descricao_msg = "OK" if pd.notna(descricao) and str(descricao).strip() != "" and len(descricao) > 500 else "Necessário preencher"
        titulo_msg = "OK" if pd.notna(titulo) and 50 <= len(str(titulo)) <= 60 else "Necessário preencher"

        atributos_produto = df_atributos[df_atributos['sku_id'] == sku_id] if df_atributos is not None else pd.DataFrame()
        atributos_vazios = atributos_produto['valor'].isna().sum() + (atributos_produto['valor'] == '').sum() if 'valor' in atributos_produto else 0
        atributos_msg = f"{atributos_vazios} campos vazios"

        erros.append({
            'sku_id': sku_id,
            'produto': titulo,
            'status': status,
            'titulo': titulo_msg,
            'qtd_imagem': qtd_imagem_msg,
            'resolucao_imagem': resolucao_msg,
            'descricao': descricao_msg,
            'garantia': garantia_erro,
            'atributos': atributos_msg
        })

    df_erros_gerais = pd.DataFrame(erros)
    return df_erros_gerais

# ------------------------- SALVAR NO BANCO DE DADOS ----------------------------

# Limpa dados antigos do vendedor
def limpar_dados_antigos(vendedor):
    conn = get_connection()
    if not conn:
        print("Erro ao conectar com o banco para limpar dados antigos.")
        return
    try:
        cursor = conn.cursor()
        tabelas = ["erros_qualidade", "variacoes", "atributos", "imagens", "produtos"]
        for tabela in tabelas:
            cursor.execute(f"DELETE FROM {tabela} WHERE vendedor = %s", (vendedor,))
        conn.commit()
        print(f"Registros antigos removidos para o vendedor {vendedor}.")
    except Exception as e:
        print(f"Erro ao limpar dados antigos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Salva os dados no banco de dados
def salvar_no_banco(produtos, imagens, atributos, variacoes, vendedor):
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
                p['sku_id'], p['titulo'], p['descricao'], p['categoria_id'], p['nome_categoria'],
                p['preco'], p['quantidade_variacoes'],
                p['status'], p['health'], p['quantidade_inicial'], p['quantidade_vendida'],
                p['quantidade_disponivel'], p['gtin'], p['marca'], p['permalink'],
                p['aceita_mercado_pago'], p['garantia'], p['imagens'], p['link_imagem'], vendedor, data_registro
            )
            for p in produtos
        ]

        query_produto = """
            INSERT INTO produtos (
                sku_id, titulo, descricao, categoria_id, nome_categoria,
                preco, quantidade_variacoes,
                status, health, quantidade_inicial, quantidade_vendida,
                quantidade_disponivel, gtin, marca, permalink,
                aceita_mercado_pago, garantia, imagens, link_imagem, vendedor, data_registro
            )
            VALUES %s
            ON CONFLICT (sku_id, vendedor) DO UPDATE SET
                titulo = EXCLUDED.titulo,
                descricao = EXCLUDED.descricao,
                categoria_id = EXCLUDED.categoria_id,
                nome_categoria = EXCLUDED.nome_categoria,
                preco = EXCLUDED.preco,
                quantidade_variacoes = EXCLUDED.quantidade_variacoes,
                status = EXCLUDED.status,
                health = EXCLUDED.health,
                quantidade_inicial = EXCLUDED.quantidade_inicial,
                quantidade_vendida = EXCLUDED.quantidade_vendida,
                quantidade_disponivel = EXCLUDED.quantidade_disponivel,
                gtin = EXCLUDED.gtin,
                marca = EXCLUDED.marca,
                permalink = EXCLUDED.permalink,
                aceita_mercado_pago = EXCLUDED.aceita_mercado_pago,
                garantia = EXCLUDED.garantia,
                imagens = EXCLUDED.imagens,
                link_imagem = EXCLUDED.link_imagem,
                vendedor = EXCLUDED.vendedor,
                data_registro = EXCLUDED.data_registro;
        """

        for i in range(0, len(produtos_valores), batch_size):
            try:
                execute_values(cursor, query_produto, produtos_valores[i:i+batch_size], page_size=batch_size)
            except Exception as e:
                print(f"Erro ao inserir batch de produtos ({i}): {e}")

        # IMAGENS
        imagens_valores = [
            (img['id_imagem'], img['sku_id'], img['secure_url'], img['resolucao'], vendedor, data_registro)
            for img in imagens
        ]

        query_imagem = """
            INSERT INTO imagens (id_imagem, sku_id, secure_url, resolucao, vendedor, data_registro)
            VALUES %s
            ON CONFLICT (id_imagem, sku_id, vendedor) DO UPDATE SET
                secure_url = EXCLUDED.secure_url,
                resolucao = EXCLUDED.resolucao,
                vendedor = EXCLUDED.vendedor,
                data_registro = EXCLUDED.data_registro;
        """

        for i in range(0, len(imagens_valores), batch_size):
            try:
                execute_values(cursor, query_imagem, imagens_valores[i:i+batch_size], page_size=batch_size)
            except Exception as e:
                print(f"Erro ao inserir batch de imagens ({i}): {e}")

        # ATRIBUTOS
        atributos_valores = [
            (a['sku_id'], a['atributo'], a['valor'], vendedor, data_registro) for a in atributos
        ]

        query_atributo = """
            INSERT INTO atributos (sku_id, atributo, valor, vendedor, data_registro)
            VALUES %s
            ON CONFLICT (sku_id, atributo, vendedor) DO UPDATE SET
                valor = EXCLUDED.valor,
                vendedor = EXCLUDED.vendedor,
                data_registro = EXCLUDED.data_registro;
        """

        for i in range(0, len(atributos_valores), batch_size):
            try:
                execute_values(cursor, query_atributo, atributos_valores[i:i+batch_size], page_size=batch_size)
            except Exception as e:
                print(f"Erro ao inserir batch de atributos ({i}): {e}")

        # VARIAÇÕES
        variacoes_valores = [
            (v['id_variacao'], v['sku_id'], v['preco_variacao'], v['atributo'], v['valor'], vendedor, data_registro)
            for v in variacoes
        ]

        query_variacao = """
            INSERT INTO variacoes (id_variacao, sku_id, preco_variacao, atributo, valor, vendedor, data_registro)
            VALUES %s
            ON CONFLICT (id_variacao, sku_id, atributo, vendedor) DO UPDATE SET
                preco_variacao = EXCLUDED.preco_variacao,
                valor = EXCLUDED.valor,
                vendedor = EXCLUDED.vendedor,
                data_registro = EXCLUDED.data_registro;
        """

        for i in range(0, len(variacoes_valores), batch_size):
            try:
                execute_values(cursor, query_variacao, variacoes_valores[i:i+batch_size], page_size=batch_size)
            except Exception as e:
                print(f"Erro ao inserir batch de variacoes ({i}): {e}")

        conn.commit()
        print("Dados salvos no banco de dados.")

    except Exception as e:
        conn.rollback()
        print(f"\nErro ao salvar no banco de dados: {e}")
    finally:
        cursor.close()
        conn.close()

# Salva os erros no banco de dados
def salvar_erros_no_banco(df_erros_gerais, vendedor):
    conn = get_connection()
    if not conn:
        print("Erro ao conectar para salvar os erros.")
        return
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_registro = datetime.now(fuso_brasilia).replace(tzinfo=None)
        valores = [
            (
                row['sku_id'], vendedor, row['produto'], row['status'], row['titulo'], 
                row['qtd_imagem'], row['resolucao_imagem'], 
                row['descricao'], row['garantia'], row['atributos'], data_registro
            )
            for _, row in df_erros_gerais.iterrows()
        ]
        query = """
            INSERT INTO erros_qualidade (
                sku_id, vendedor, produto, status, titulo, 
                qtd_imagem, resolucao_imagem, 
                descricao, garantia, atributos, data_registro
            ) VALUES %s;
        """

        execute_values(cursor, query, valores)
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar erros no banco: {e}")
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
            SELECT * FROM produtos WHERE vendedor = %s AND DATE(data_registro) = %s
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
            SELECT * FROM atributos WHERE vendedor = %s AND DATE(data_registro) = %s
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
            SELECT * FROM imagens WHERE vendedor = %s AND DATE(data_registro) = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# Busca as variações do dia para download
def buscar_variacoes_do_dia(vendedor):
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM variacoes WHERE vendedor = %s AND DATE(data_registro) = %s
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
        return []
    try:
        cursor = conn.cursor()
        fuso_brasilia = pytz.timezone("America/Sao_Paulo")
        data_hoje = datetime.now(fuso_brasilia)
        cursor.execute("""
            SELECT * FROM erros_qualidade WHERE vendedor = %s AND DATE(data_registro) = %s
        """, (vendedor, data_hoje.date()))
        colunas = [desc[0] for desc in cursor.description]
        return [dict(zip(colunas, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

# ------------------------- GERAR XLSX E ZIP----------------------------

# Gera um arquivo ZIP com os relatórios do dia
def gerar_zip_relatorios_do_dia(vendedor):

    produtos = buscar_produtos_do_dia(vendedor)
    atributos = buscar_atributos_do_dia(vendedor)
    imagens = buscar_imagens_do_dia(vendedor)
    variacoes = buscar_variacoes_do_dia(vendedor)
    erros = buscar_erros_do_dia(vendedor)

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
        if variacoes:
            df_variacoes = pd.DataFrame(variacoes)
            zf.writestr(f"variacoes_{vendedor}.xlsx", df_to_xlsx_bytes(df_variacoes))
        if erros:
            df_erros = pd.DataFrame(erros)
            zf.writestr(f"erros_gerais_{vendedor}.xlsx", df_to_xlsx_bytes(df_erros))
    zip_stream.seek(0)
    return zip_stream

# -------------------------------- EXECUÇÃO PRINCIPAL --------------------------------

# Função principal para coletar dados do Mercado Livre
def coletar_dados_ml(vendedor: str):

    print(f"\nIniciando coleta Mercado Livre para o vendedor: {vendedor}")

    # Verifica se o vendedor está na lista de tokens
    tokens = load_tokens()
    if vendedor not in tokens:
        msg = f"Vendedor '{vendedor}' não encontrado."
        print(msg)
        raise Exception(msg)

    # Obtém os tokens do vendedor
    seller_data = tokens[vendedor]
    access_token = seller_data['access_token']
    refresh_token = seller_data['refresh_token']
    seller_id = seller_data['seller_id']

    # Obtém todos os dados
    produtos, imagens, atributos, variacoes = obter_todos_os_dados(
        seller_id, access_token, refresh_token, vendedor
    )
    salvar_no_banco(produtos, imagens, atributos, variacoes, vendedor)

    df_produtos = pd.DataFrame(produtos)
    df_imagens = pd.DataFrame(imagens)
    df_atributos = pd.DataFrame(atributos)
    df_erros = tratar_dados(df_produtos, df_imagens, df_atributos)
    salvar_erros_no_banco(df_erros, vendedor)

    print(f"\nColeta Mercado Livre finalizada para {vendedor}")
    return f"\nColeta Mercado Livre finalizada para {vendedor}"

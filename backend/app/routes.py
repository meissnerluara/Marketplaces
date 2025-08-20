from fastapi import APIRouter, Request, HTTPException, Depends, status
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from app.services import magalu, mercadolivre, amazon
from app.services.utils import load_tokens_from_env
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import secrets
import time
from datetime import datetime, timedelta

load_dotenv()

router = APIRouter()

# Configurações de segurança
SECRET_KEY = os.getenv("JWT_SECRET", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Contexto de criptografia para senhas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class Token(BaseModel):
    access_token: str
    token_type: str

class PasswordInput(BaseModel):
    password: str

class ColetaRequest(BaseModel):
    plataforma: str
    vendedor: str

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def authenticate_user(password: str):
    hashed_password = os.getenv("APP_PASSWORD_HASH")
    if not hashed_password:
        hashed_password = get_password_hash(os.getenv("APP_PASSWORD"))
        os.environ["APP_PASSWORD_HASH"] = hashed_password
    
    return verify_password(password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise credentials_exception

@router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    if not authenticate_user(form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Senha incorreta",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(
        data={"sub": "admin"}, 
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/coletar")
async def coletar(
    request: ColetaRequest, 
    current_user: dict = Depends(get_current_user)
):
    try:
        if request.plataforma == "magalu":
            msg = magalu.coletar_dados_magalu(request.vendedor)
        elif request.plataforma == "mercadolivre":
            msg = mercadolivre.coletar_dados_ml(request.vendedor)
        elif request.plataforma == "amazon":
            msg = amazon.coletar_dados_amazon(request.vendedor)
        else:
            return {"erro": "Plataforma não suportada"}
        return {"mensagem": msg}
    except Exception:
        return {"erro": "Falha na operação de coleta"}

@router.get("/vendedores/{plataforma}")
def listar_vendedores(
    plataforma: str, 
    current_user: dict = Depends(get_current_user)
):
    try:
        tokens = load_tokens_from_env(plataforma)
        return list(tokens.keys())
    except Exception:
        return {"erro": "Falha ao listar vendedores"}

@router.get("/download")
def baixar_zip(
    plataforma: str, 
    vendedor: str, 
    current_user: dict = Depends(get_current_user)
):
    try:
        if plataforma == "magalu":
            zip_stream = magalu.gerar_zip_relatorios_do_dia(vendedor)
            nome_base = f"Magalu_{vendedor}"
        elif plataforma == "mercadolivre":
            zip_stream = mercadolivre.gerar_zip_relatorios_do_dia(vendedor)
            nome_base = f"MercadoLivre_{vendedor}"
        elif plataforma == "amazon":
            zip_stream = amazon.gerar_zip_relatorios_do_dia(vendedor)
            nome_base = f"Amazon_{vendedor}"
        else:
            return {"erro": "Plataforma não suportada"}

        return StreamingResponse(
            zip_stream,
            media_type="application/x-zip-compressed",
            headers={"Content-Disposition": f"attachment; filename={nome_base}_Relatorios.zip"}
        )
    except Exception:
        return {"erro": "Falha ao gerar relatório"}

@router.get("/stream_logs")
async def stream_logs(
    plataforma: str, 
    vendedor: str, 
    current_user: dict = Depends(get_current_user)
):
    def fake_event():
        yield f"data: Coleta finalizada para {vendedor} na plataforma {plataforma}\n\n"
    return StreamingResponse(fake_event(), media_type="text/event-stream")

from fastapi import APIRouter, Depends, HTTPException, status, Form, Cookie, Response
from fastapi.responses import JSONResponse
from db.models.usuario import Usuario
from typing import Optional
import jwt
from jwt import ExpiredSignatureError as TokenExpiredError
from datetime import datetime, timedelta
from uuid import uuid4
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm
from google.cloud import bigquery
from pytz import timezone

router = APIRouter(prefix='/rank_tracker_usuarios',
                   tags=['rank tracker usuarios'],
                   responses={404:{"message":"No encontrado"}})

class TokenData:
    username: str = None

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"  # Cambia esto por una clave secreta segura
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

fmt = "%Y-%m-%d"
clientbq = bigquery.Client()

credentials_exception = HTTPException(
    status_code=401, 
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

# Genera el access token
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@router.get("/refresh-token/")
async def verify_refresh_token(response: Response, refresh_token: Optional[str] = Cookie(None)):
    print(refresh_token)
    if not refresh_token:
        #raise HTTPException(status_code=401, detail="No token present")
        return {"message": "logout"}
    # Usar tu función de validación de token existente aquí
    try:
        payload = await verify_refresh_token_bq(refresh_token)
    except TokenExpiredError:
        # Si el token ha expirado, puedes optar por emitir uno nuevo aquí y devolverlo
        refresh_token = str(uuid4())
        await store_refresh_token_in_bigquery(payload.username, refresh_token)
        response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, samesite="none", secure=True)
        return {"message": "Token has been refreshed"}
    except Exception as e:
        return {"message": "logout"}
        #raise HTTPException(status_code=401, detail="Invalid token")
    return {"message": payload['name']}

@router.post("/register/")
async def register_user(usuario: Usuario):
    # Asegurarse de que el usuario no exista previamente. 
    # Para ello, puedes usar una versión modificada de `get_user_from_bigquery` o hacer una consulta directa aquí.
    existing_user = await get_user_from_bigquery(usuario.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    # Insertar el nuevo usuario en BigQuery
    await insert_user_into_bigquery(usuario.name, usuario.username, usuario.password, usuario.keywords_count, usuario.user_type)
    
    return {"message": "User registered successfully"}

@router.get("/loginv3/", status_code=status.HTTP_202_ACCEPTED)
async def login(username:str, password:str):
    user = await get_user_from_bigquery(username)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    if not verify_password(password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    print(user)
    return {"keywords_count":user["keywords_count"], "user_type":user["user_type"]}

@router.post("/loginv2/")
async def login(response: Response, username: str = Form(...), password: str = Form(...)):
    # Obtener el usuario de BigQuery usando el mail
    user = await get_user_from_bigquery(username)

    # Si el usuario no existe o las contraseñas no coinciden
    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    if not verify_password(password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    access_token = create_access_token(data={"sub": username})
    response.set_cookie(key="accessToken", value=access_token, httponly=True)
    response.set_cookie(key="userEmail", value=user['username'], httponly=True)
    response.set_cookie(key="userName", value=user['name'], httponly=True)
    return {"accessToken": access_token, "mail": user['username'], "name": user['name']}

@router.post("/logoutv2/")
async def logout(response: Response, accessToken: Optional[str] = Cookie(None)):
    response.set_cookie(key="accessToken", httponly=True, value="", samesite="none", secure=True, max_age=0)
    response.set_cookie(key="userEmail", httponly=True, value="", samesite="none", secure=True, max_age=0)
    response.set_cookie(key="userName", httponly=True, value="", samesite="none", secure=True, max_age=0)
    return {"message": "logout"}

@router.post("/token/")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await get_user_from_bigquery(form_data.username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    
    if not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    # Crear el refresh token y guardar en BigQuery junto al usuario
    refresh_token = str(uuid4())
    await store_refresh_token_in_bigquery(form_data.username, refresh_token)
    
    access_token = create_access_token(data={"sub": form_data.username})
    response = JSONResponse(content={"access_token": access_token, "token_type": "bearer"})
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, max_age=3600*24*15)  # Valido por 15 días
    return response

@router.post("/logout/")
async def logout(response: Response, refresh_token: Optional[str] = Cookie(None)):
    if not refresh_token:
        raise HTTPException(status_code=400, detail="Refresh token missing")
    
    # Aquí, si estás guardando los tokens en una base de datos, debes eliminar el token correspondiente
    await delete_refresh_token_in_bigquery(refresh_token)

    # Establece la cookie a una cadena vacía y configura la expiración para que sea en el pasado, esto elimina la cookie
    response.set_cookie(key="refresh_token", value="", httponly=True, samesite="none", secure=True, max_age=0)
    
    return {"detail": "Successfully logged out"}


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

async def verify_refresh_token_bq(refresh_token: str):
    query = f"SELECT username, name FROM `neo-rank-tracker.rank_tracker_neo.tbl_usuarios` WHERE refresh_token = @refresh_token"
    query_params = [
        bigquery.ScalarQueryParameter("refresh_token", "STRING", refresh_token)]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    results = clientbq.query(query, job_config=job_config)
    for row in results:
        return {
            "username": row["username"],
            "name": row["name"]
        }
    return None

async def get_user_from_bigquery(username: str):
    # Tu lógica para buscar el usuario en BigQuery
    query = f"SELECT username, hashed_password, name, keywords_count, user_type FROM `neo-rank-tracker.rank_tracker_neo.tbl_usuarios` WHERE username = @username"
    query_params = [
        bigquery.ScalarQueryParameter("username", "STRING", username)]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    results = clientbq.query(query, job_config=job_config)
    for row in results:
        return {
            "username": row["username"],
            "hashed_password": row["hashed_password"],
            "name": row["name"],
            "keywords_count": row["keywords_count"],
            "user_type": row["user_type"]
            # ... otros campos si es necesario
        }
    return None

async def store_refresh_token_in_bigquery(username: str, refresh_token: str):
    # Aquí, también estoy asumiendo que quieras almacenar la fecha de creación del token.
    # Esto podría ser útil para implementar una expiración del refresh token.
    nowutc = datetime.now(timezone('GMT'))
    now_peru = nowutc.astimezone(timezone('America/Bogota'))
    creation_date = now_peru.strftime(fmt)

    query = f"UPDATE `neo-rank-tracker.rank_tracker_neo.tbl_usuarios` SET refresh_token = @refresh_token , token_creation_date = '{creation_date}' WHERE username = @username;"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("refresh_token", "STRING", refresh_token)
        ]
    )
    clientbq.query(query, job_config=job_config)

async def delete_refresh_token_in_bigquery(refresh_token: str):
    query = f"UPDATE `neo-rank-tracker.rank_tracker_neo.tbl_usuarios` SET refresh_token = '' WHERE refresh_token = @refresh_token;"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("refresh_token", "STRING", refresh_token)
        ]
    )
    clientbq.query(query, job_config=job_config)

def create_hashed_password(password: str) -> str:
    return pwd_context.hash(password)

async def insert_user_into_bigquery(name: str, username: str, password: str, keywords_count: int, user_type: str):
    hashed_password = create_hashed_password(password)
    query = f"INSERT INTO `neo-rank-tracker.rank_tracker_neo.tbl_usuarios` (name, username, hashed_password, user_type, keywords_count) VALUES (@name, @username, @hashed_password, @usertype, @keywords_count);"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("name", "STRING", name),
            bigquery.ScalarQueryParameter("hashed_password", "STRING", hashed_password),
            bigquery.ScalarQueryParameter("usertype", "STRING", user_type),
            bigquery.ScalarQueryParameter("keywords_count", "INTEGER", keywords_count)
        ]
    )
    clientbq.query(query, job_config=job_config)

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
        return token_data
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid token",
        )

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
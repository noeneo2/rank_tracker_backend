from fastapi import APIRouter, Depends, HTTPException, status, Form, Cookie, Response
from fastapi.responses import JSONResponse
from db.models.usuario import Usuario
from db.firestore_client import get_users_collection
from typing import Optional
import jwt
from jwt import ExpiredSignatureError as TokenExpiredError
from datetime import datetime, timedelta
from uuid import uuid4
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm
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
        return {"message": "logout"}
    try:
        payload = await verify_refresh_token_firestore(refresh_token)
    except TokenExpiredError:
        refresh_token = str(uuid4())
        await store_refresh_token(payload.username, refresh_token)
        response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, samesite="none", secure=True)
        return {"message": "Token has been refreshed"}
    except Exception as e:
        return {"message": "logout"}
    return {"message": payload['name']}

@router.post("/register/")
async def register_user(usuario: Usuario):
    """Register a new user in Firestore"""
    existing_user = await get_user(usuario.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    await insert_user(usuario.name, usuario.username, usuario.password, usuario.keywords_count, usuario.user_type)
    
    return {"message": "User registered successfully"}

@router.get("/loginv3/", status_code=status.HTTP_202_ACCEPTED)
async def login(username: str, password: str):
    user = await get_user(username)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    if not verify_password(password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    print(user)
    return {"keywords_count": user["keywords_count"], "user_type": user["user_type"]}

@router.post("/loginv2/")
async def login(response: Response, username: str = Form(...), password: str = Form(...)):
    user = await get_user(username)

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
    user = await get_user(form_data.username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    
    if not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    refresh_token = str(uuid4())
    await store_refresh_token(form_data.username, refresh_token)
    
    access_token = create_access_token(data={"sub": form_data.username})
    response = JSONResponse(content={"access_token": access_token, "token_type": "bearer"})
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, max_age=3600*24*15)
    return response

@router.post("/logout/")
async def logout(response: Response, refresh_token: Optional[str] = Cookie(None)):
    if not refresh_token:
        raise HTTPException(status_code=400, detail="Refresh token missing")
    
    await delete_refresh_token(refresh_token)
    response.set_cookie(key="refresh_token", value="", httponly=True, samesite="none", secure=True, max_age=0)
    
    return {"detail": "Successfully logged out"}


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# ==================== FIRESTORE FUNCTIONS ====================

async def verify_refresh_token_firestore(refresh_token: str):
    """Verify refresh token by querying Firestore"""
    users_ref = get_users_collection()
    query = users_ref.where('refresh_token', '==', refresh_token).limit(1)
    docs = query.stream()
    
    for doc in docs:
        data = doc.to_dict()
        return {
            "username": data.get("username"),
            "name": data.get("name")
        }
    return None

async def get_user(username: str):
    """Get user from Firestore by username"""
    users_ref = get_users_collection()
    doc_ref = users_ref.document(username)
    doc = doc_ref.get()
    
    if doc.exists:
        data = doc.to_dict()
        return {
            "username": data.get("username"),
            "hashed_password": data.get("hashed_password"),
            "name": data.get("name"),
            "keywords_count": data.get("keywords_count"),
            "user_type": data.get("user_type")
        }
    return None

async def store_refresh_token(username: str, refresh_token: str):
    """Store refresh token in Firestore"""
    nowutc = datetime.now(timezone('GMT'))
    now_peru = nowutc.astimezone(timezone('America/Bogota'))
    creation_date = now_peru.strftime(fmt)

    users_ref = get_users_collection()
    doc_ref = users_ref.document(username)
    doc_ref.update({
        "refresh_token": refresh_token,
        "token_creation_date": creation_date
    })

async def delete_refresh_token(refresh_token: str):
    """Delete refresh token from Firestore"""
    users_ref = get_users_collection()
    query = users_ref.where('refresh_token', '==', refresh_token).limit(1)
    docs = query.stream()
    
    for doc in docs:
        doc.reference.update({"refresh_token": ""})

def create_hashed_password(password: str) -> str:
    return pwd_context.hash(password)

async def insert_user(name: str, username: str, password: str, keywords_count: int, user_type: str):
    """Insert new user into Firestore"""
    hashed_password = create_hashed_password(password)
    
    users_ref = get_users_collection()
    doc_ref = users_ref.document(username)  # Use username as document ID
    doc_ref.set({
        "name": name,
        "username": username,
        "hashed_password": hashed_password,
        "user_type": user_type,
        "keywords_count": keywords_count,
        "refresh_token": None,
        "token_creation_date": None
    })

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
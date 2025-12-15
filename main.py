from fastapi import FastAPI
from routers import proyectos, usuarios
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

origins = [
    "https://rank-tracker-empati.netlify.app",
        "https://suite-neo-plus.netlify.app",
        "https://rank-tracker-v4.netlify.app",
        "http://localhost:4200/"
]

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*']
    )
]

app = FastAPI(middleware=middleware, docs_url="/documentation", redoc_url="/reference")
app.include_router(proyectos.router)
app.include_router(usuarios.router)
app.mount('/static', StaticFiles(directory="static"), name="static")

@app.get("/")
async def main():
    return {"message": "Hola mundo!"}
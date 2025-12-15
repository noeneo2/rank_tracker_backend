from pydantic import BaseModel
from typing import List, Optional

class Proyecto(BaseModel):
    nombre_proyecto: str
    dominio_princpal: str
    subdomain_enabled: bool
    idioma: str
    pais: str
    coordenadas: str
    competidores: list
    keywords: list
    paid_enabled: bool
    estado : int
    usuario: str
    project_id: str
    fecha_creaciones: Optional[str] = None


class ProyectoDetalle(BaseModel):
    nombre_proyecto: str
    dominio_princpal: str
    subdomain_enabled: bool
    idioma: str
    pais: str
    competidores: List[str]
    estado: int
    paid_enabled: bool

class TaskRequest(BaseModel):
    fecha: str
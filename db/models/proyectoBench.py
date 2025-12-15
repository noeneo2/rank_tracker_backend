from pydantic import BaseModel

class ProyectoBench(BaseModel):
    project_name: str
    id_project: str
    idioma: str
    pais: str
    coordenadas: str
    keywords: list
    track_paid: bool
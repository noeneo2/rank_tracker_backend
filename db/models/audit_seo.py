# db/models/audit_seo.py
from pydantic import BaseModel
from typing import Optional

class AuditSEOCreateRequest(BaseModel):
    """
    Modelo Pydantic para la solicitud de creación de un proyecto de Auditoría SEO.
    Define los campos esperados del formulario de creación.
    """
    nombre_cliente: str
    dominio_web: str
    render_web: bool
    tracking_semanal: bool
    has_sitemap: bool
    sitemap_url: Optional[str] = None  # Opcional, solo si has_sitemap es True
    max_crawl_pages: int
    usuario: str # Usuario propietario del proyecto

class AuditSEOProject(BaseModel):
    """
    Modelo Pydantic para representar un proyecto de Auditoría SEO en la base de datos.
    """
    nombre_cliente: str
    dominio_web: str
    render_web: bool
    tracking_semanal: bool
    has_sitemap: bool
    sitemap_url: Optional[str] = None
    max_crawl_pages: int
    usuario: str
    project_id: str # ID único del proyecto generado por DataForSEO (o BigQuery)
    fecha_creacion: str # Fecha de creación del proyecto
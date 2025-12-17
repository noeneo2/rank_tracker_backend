"""
Script de migración de datos de BigQuery a Firestore.
Migra usuarios (tbl_usuarios) y proyectos (tbl_proyecto) a Firestore.

Ejecutar con: python db/migrate_to_firestore.py
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
from db.firestore_client import get_users_collection, get_projects_collection

# Ruta al archivo de credenciales
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '..', 'routers', 'neo-rank-tracker-63b755f3c88a.json')

def get_bigquery_client():
    """Create BigQuery client with explicit credentials"""
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return bigquery.Client(credentials=credentials, project='neo-rank-tracker')

def migrate_users():
    """Migra usuarios de BigQuery a Firestore."""
    print("=== MIGRANDO USUARIOS ===")
    
    client = get_bigquery_client()
    query = "SELECT * FROM `neo-rank-tracker.rank_tracker_neo.tbl_usuarios`"
    results = client.query(query).result()
    
    users_ref = get_users_collection()
    count = 0
    
    for row in results:
        user_data = {
            "name": row.get("name", ""),
            "username": row.get("username", ""),
            "hashed_password": row.get("hashed_password", ""),
            "keywords_count": row.get("keywords_count", 0),
            "user_type": row.get("user_type", "user"),
            "refresh_token": row.get("refresh_token", None),
            "token_expires_at": row.get("token_expires_at", None),
        }
        
        # Usa username como document ID
        doc_ref = users_ref.document(user_data["username"])
        doc_ref.set(user_data)
        count += 1
        print(f"  Migrado usuario: {user_data['username']}")
    
    print(f"✅ {count} usuarios migrados exitosamente.\n")
    return count


def migrate_projects():
    """Migra proyectos de BigQuery a Firestore."""
    print("=== MIGRANDO PROYECTOS ===")
    
    client = get_bigquery_client()
    query = "SELECT * FROM `neo-rank-tracker.rank_tracker_neo.tbl_proyecto`"
    results = client.query(query).result()
    
    projects_ref = get_projects_collection()
    count = 0
    skipped_projects = []  # Track projects that couldn't be migrated
    
    import json
    
    for row in results:
        # Parsear keywords y competidores
        keywords = row.get("keywords", [])
        competidores = row.get("competidores", [])
        
        # Convertir a lista si es string JSON
        if isinstance(keywords, str):
            try:
                keywords = json.loads(keywords)
            except:
                keywords = []
        
        if isinstance(competidores, str):
            try:
                competidores = json.loads(competidores)
            except:
                competidores = []
        
        # Firestore no permite nested arrays, así que convertimos cada keyword a un objeto
        # Estructura original: ["keyword", "categoria", "subcategoria", "intencion", volumen]
        keywords_objects = []
        if keywords:
            for kw in keywords:
                if isinstance(kw, list) and len(kw) >= 1:
                    # Convertir array a objeto/diccionario
                    kw_obj = {
                        "keyword": kw[0] if len(kw) > 0 else "",
                        "categoria": kw[1] if len(kw) > 1 else "",
                        "subcategoria": kw[2] if len(kw) > 2 else "",
                        "intencion": kw[3] if len(kw) > 3 else "",
                        "volumen": kw[4] if len(kw) > 4 else 0
                    }
                    keywords_objects.append(kw_obj)
                elif isinstance(kw, dict):
                    # Ya es un diccionario, usarlo directamente
                    keywords_objects.append(kw)
                elif isinstance(kw, str):
                    # Es solo un string, crear objeto simple
                    keywords_objects.append({"keyword": kw})
        
        # Lo mismo para competidores (usualmente son strings simples o arrays)
        competidores_list = []
        if competidores:
            for comp in competidores:
                if isinstance(comp, list) and len(comp) > 0:
                    # Si es un array, tomar el primer elemento como dominio
                    competidores_list.append(comp[0] if isinstance(comp[0], str) else str(comp[0]))
                elif isinstance(comp, str):
                    competidores_list.append(comp)
                elif isinstance(comp, dict):
                    # Si es un dict, extraer el dominio o convertir a string
                    competidores_list.append(comp.get("dominio", json.dumps(comp)))
                else:
                    competidores_list.append(str(comp))
        
        project_data = {
            "nombre_proyecto": row.get("nombre_proyecto", ""),
            "dominio_princpal": row.get("dominio_princpal", ""),
            "subdomain_enabled": bool(row.get("subdomain_enabled", False)),
            "idioma": row.get("idioma", ""),
            "pais": row.get("pais", ""),
            "keywords": keywords_objects,
            "competidores": competidores_list,
            "fecha_creaciones": row.get("fecha_creaciones", ""),
            "estado": row.get("estado", 0),
            "coordenadas": row.get("coordenadas", ""),
            "paid_enabled": bool(row.get("paid_enabled", False)),
            "user_owner": row.get("user_owner", ""),
            "project_id": row.get("project_id", ""),
        }
        
        # Usa project_id como document ID
        doc_id = project_data["project_id"] or project_data["nombre_proyecto"]
        doc_ref = projects_ref.document(doc_id)
        
        try:
            doc_ref.set(project_data)
            count += 1
            print(f"  ✅ Migrado proyecto: {project_data['nombre_proyecto'][:50]}...")
        except Exception as e:
            error_msg = str(e)
            if "exceeds the maximum allowed size" in error_msg:
                print(f"  ⚠️ SALTADO (muy grande): {project_data['nombre_proyecto'][:50]} - {len(keywords_objects)} keywords")
                skipped_projects.append({
                    "id": doc_id,
                    "nombre": project_data["nombre_proyecto"],
                    "keywords_count": len(keywords_objects),
                    "reason": "Documento muy grande (>1MB)"
                })
            else:
                print(f"  ❌ ERROR: {project_data['nombre_proyecto'][:50]} - {error_msg[:100]}")
                skipped_projects.append({
                    "id": doc_id,
                    "nombre": project_data["nombre_proyecto"],
                    "reason": error_msg[:200]
                })
    
    print(f"\n✅ {count} proyectos migrados exitosamente.")
    if skipped_projects:
        print(f"⚠️ {len(skipped_projects)} proyectos saltados por ser muy grandes:")
        for p in skipped_projects:
            print(f"   - {p['nombre'][:60]} ({p.get('keywords_count', '?')} keywords)")
    print()
    return count, skipped_projects


if __name__ == "__main__":
    print("\n" + "="*50)
    print("MIGRACIÓN DE BIGQUERY A FIRESTORE")
    print("="*50 + "\n")
    
    try:
        # Usuarios ya migrados, solo migrar proyectos
        # users_count = migrate_users()
        users_count = 31  # Ya migrados previamente
        print("✅ Usuarios ya migrados previamente (31)\n")
        
        projects_count, skipped = migrate_projects()
        
        print("="*50)
        print("RESUMEN DE MIGRACIÓN")
        print("="*50)
        print(f"Usuarios migrados: {users_count}")
        print(f"Proyectos migrados: {projects_count}")
        if skipped:
            print(f"Proyectos saltados: {len(skipped)} (muy grandes para Firestore)")
        print("\n✅ Migración completada!")
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()

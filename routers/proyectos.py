from fastapi import APIRouter, status, HTTPException, BackgroundTasks
from db.models.proyecto import Proyecto, TaskRequest
from db.models.audit_seo import AuditSEOCreateRequest, AuditSEOProject
from db.models.proyectoBench import ProyectoBench
from db import dataforseoClient
from db.firestore_client import get_projects_collection, get_users_collection
import os
from google.cloud import bigquery
from pytz import timezone
from datetime import datetime
import json
import urllib.parse
from urllib.parse import quote
import tldextract
from datetime import date
from google.api_core.exceptions import GoogleAPIError
import traceback
import aiohttp
import asyncio
from aiohttp import ClientTimeout
import requests
import httpx
from retrying import retry
from typing import Union

router = APIRouter(prefix='/rank_tracker',
                   tags=['rank tracker proyectos'],
                   responses={404:{"message":"No encontrado"}})

client = dataforseoClient.RestClient("accounts@neo.com.pe", "sKFJCQjSl8AKRZg5")

# Use credentials file if available (local dev), otherwise use default service account (Cloud Run)
creds_file = 'routers/neo-rank-tracker-63b755f3c88a.json'
if os.path.exists(creds_file):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file
    print("Using local credentials file")
else:
    print("Using default Cloud Run service account")

fmt = "%Y-%m-%d"
clientbq = bigquery.Client()

@retry(stop_max_attempt_number=5, wait_fixed=2000)
def obtener_proyectos(task_id: str, project_id: str):
    clientbq = bigquery.Client()
    query = """
    SELECT nombre_proyecto, keyword, categoria, subcategoria, intencion, volumen, subdomains_enabled, dominios, dominio_principal, paid_enabled, project_id 
    FROM `neo-rank-tracker.rank_tracker_neo.tbl_keywords_pre_request` 
    WHERE taskId = @taskId and project_id = @project_id
    """
    query_params = [
        bigquery.ScalarQueryParameter("taskId", "STRING", task_id),
        bigquery.ScalarQueryParameter("project_id", "STRING", project_id)
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = clientbq.query(query, job_config=job_config)
    results = query_job.result()

    row = next(iter(results), None)
    if not row:
        raise ValueError(f"No se encontraron proyectos para taskId={task_id}, project_id={project_id}")

    dominios = json.loads(row['dominios']) if isinstance(row['dominios'], str) else row['dominios']
    proyecto = {
        "nombre_proyecto": row['nombre_proyecto'],
        "keyword": row['keyword'],
        "categoria": row['categoria'],
        "subcategoria": row['subcategoria'],
        "intencion": row['intencion'],
        "volumen": row['volumen'],
        "subdomains_enabled": row['subdomains_enabled'],
        "dominios": dominios,
        "dominio_principal": row['dominio_principal'],
        "paid_enabled": row['paid_enabled'],
        "project_id": row['project_id']
    }

    return proyecto

@router.post('/missing_tasks/', status_code=status.HTTP_202_ACCEPTED)
async def process_missing_tasks(request: TaskRequest, background_tasks: BackgroundTasks):
    # Aquí llamas a tu función asincrónica para procesar las tareas
    try:
        background_tasks.add_task(runMissingTasks_async, request.fecha)
    except HTTPException as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"message": "Proceso iniciado correctamente para la fecha: " + request.fecha}

async def runMissingTasks_async(fecha: str):
    dataset_id = 'rank_tracker_neo'
    table_id = 'tbl_keywords_pre_request'
    table_ref = clientbq.dataset(dataset_id).table(table_id)
    table = clientbq.get_table(table_ref)

    sql = f"""
            SELECT nombre_proyecto, taskId 
            FROM `neo-rank-tracker.rank_tracker_neo.tbl_keywords_pre_request` 
            WHERE fecha = '{fecha}';
          """
    print(sql)
    # Ejecutar la consulta en BigQuery
    query_job = clientbq.query(sql)
    results = query_job.result()
    async with aiohttp.ClientSession() as session:
        htmls = await fetch_all(session, results)
        print(htmls)

async def fetch_all(s, results):
    tasks = []
    for row in results:
        task = asyncio.create_task(fetch(s,row))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res

async def fetch(s, row):
    nombre_proyecto, taskId = row.nombre_proyecto, row.taskId
    timeout = ClientTimeout(total=3600)
    async with s.get(f"http://127.0.0.1:8001/rank_process/obtener/?id={taskId}&tag={urllib.parse.quote(nombre_proyecto)}", timeout=timeout) as r:
        if r.status != 200:
            r.raise_for_status()
        return await r.text()

@router.post('/crear-bench-seo/',status_code=status.HTTP_202_ACCEPTED)
async def crearProyectoBenchSEO(proyectoB: ProyectoBench, background_tasks: BackgroundTasks):
    try:
        print(f'--->> BENCHMARK SEO <<---')
        dataset_id = 'rank_tracker_bench_seo'
        table_id = 'tbl_proyecto_bench_seo'
        table_ref = clientbq.dataset(dataset_id).table(table_id)
        table = clientbq.get_table(table_ref)
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_funcion = now_peru.strftime(fmt)
        rows_to_insert = [(proyectoB.project_name, proyectoB.idioma, proyectoB.pais, proyectoB.coordenadas, json.dumps(proyectoB.keywords), bool(proyectoB.track_paid), fecha_funcion)]
        print(rows_to_insert)
        errors = clientbq.insert_rows(table, rows_to_insert)
        if errors == []:
            print('Proyecto guardado')
        else:
            print(errors)
        background_tasks.add_task(crearBenchProyect, proyectoB.project_name, proyectoB.idioma, proyectoB.pais, proyectoB.coordenadas, json.dumps(proyectoB.keywords), bool(proyectoB.track_paid), fecha_funcion)
        
        return {'message':'Ok'}
    except HTTPException as error:
        raise HTTPException(status_code=400, detail=str(error))

@router.get('/listar_proyectos_bench/')
def listarProyectosAngularBench(username: str):
    print(username)
    try:
        sql = f"SELECT nombre_proyecto, idioma, pais FROM `neo-rank-tracker.rank_tracker_bench_seo.tbl_proyecto_bench_seo`"
        print(sql)
        df = clientbq.query(sql).to_dataframe()
        datajson = {"items":[]}
        for ind in df.index:
            nombre_proyecto = df['nombre_proyecto'][ind]
            idioma = df['idioma'][ind]
            pais = df['pais'][ind]
            item = {"project_name":nombre_proyecto, "idioma":idioma, "pais":pais}
            datajson["items"].append(item)
        proyectos = datajson['items']
        return proyectos
    except:
        return {'message':'error'}


@router.post('/ejecutar_proyecto/', status_code=status.HTTP_202_ACCEPTED)
async def ejecutarProyectoManual(project_id: str, background_tasks: BackgroundTasks):
    try:
        print(project_id)
        
        # Get project from Firestore
        projects_ref = get_projects_collection()
        doc = projects_ref.document(project_id).get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"No se encontró el proyecto: {project_id}")

        row = doc.to_dict()
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_funcion = now_peru.strftime(fmt)

        keywords = row.get('keywords', [])
        competidores = row.get('competidores', [])
        
        # Ensure keywords/competidores are lists (they should be in Firestore)
        if isinstance(keywords, str):
            keywords = json.loads(keywords)
        if isinstance(competidores, str):
            competidores = json.loads(competidores)

        proyecto = {
            "nombre_proyecto": row["nombre_proyecto"],
            "dominio_princpal": row["dominio_princpal"],
            "subdomain_enabled": bool(row["subdomain_enabled"]),
            "idioma": row["idioma"],
            "project_id": row["project_id"],
            "coordenadas": row["coordenadas"],
            "competidores": competidores,
            "keywords": keywords,
            "paid_enabled": bool(row["paid_enabled"]),
        }

        # Llama a la tarea en segundo plano
        background_tasks.add_task(
            crearFirstProyect,
            proyecto["nombre_proyecto"],
            proyecto["dominio_princpal"],
            proyecto["subdomain_enabled"],
            proyecto["idioma"],
            proyecto["keywords"],
            proyecto["competidores"],
            fecha_funcion,
            proyecto["coordenadas"],
            proyecto["paid_enabled"],
            proyecto["project_id"]
        )

        return {'message': 'ok'}
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Ha ocurrido un error: {error}")




@router.get('/keywords_disponible/',status_code=status.HTTP_202_ACCEPTED)
async def keywordsDisponibles(username: str):
    print(username)
    try:
        keyword_count_user = 0
        keyword_count_total = 0
        
        # Get user's keywords count from Firestore projects
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', username).where('estado', '==', 1)
        docs = query.stream()
        
        for doc in docs:
            data = doc.to_dict()
            keywords = data.get('keywords', [])
            keyword_count_user += len(keywords)
        
        # Get user's total allowed keywords from Firestore users
        users_ref = get_users_collection()
        user_doc = users_ref.document(username).get()
        if user_doc.exists:
            user_data = user_doc.to_dict()
            keyword_count_total = user_data.get('keywords_count', 0)
        
        keywords_disponibles = int(keyword_count_total) - int(keyword_count_user)
        print(keywords_disponibles)
        return {"keywords":keywords_disponibles}
    except HTTPException as error:
        raise HTTPException(status_code=400, detail=str(error))

@router.post('/crear/',status_code=status.HTTP_202_ACCEPTED)
async def crearProyecto(proyecto: Proyecto, background_tasks: BackgroundTasks):
    try:
        print(f'--->> CREACION DE PROYECTO INICIADO <<---')
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_funcion = now_peru.strftime(fmt)
        keyword_count_total = 0
        keyword_count_user = 0
        
        print(f'--->> VALIDANDO LA CANTIDAD DE KEYWORDS DISPONIBLES <<---')
        
        # Get user's keywords count from Firestore projects
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', proyecto.usuario).where('estado', '==', 1)
        docs = query.stream()
        
        for doc in docs:
            data = doc.to_dict()
            keywords = data.get('keywords', [])
            keyword_count_user += len(keywords)
        
        # Get user's total allowed keywords from Firestore users
        users_ref = get_users_collection()
        user_doc = users_ref.document(proyecto.usuario).get()
        if user_doc.exists:
            user_data = user_doc.to_dict()
            keyword_count_total = user_data.get('keywords_count', 0)
        
        keywords_availables = keyword_count_total - keyword_count_user
        if((keywords_availables) <= 0):
            return {
                "status": "error",
                "code": "NO_KEYWORDS_AVAILABLE",
                "message": "No tienes suficientes keywords disponibles para crear un nuevo proyecto."
            }
        else:
            if (keywords_availables - len(proyecto.keywords) <= 0):
                print(f"No puede crear proyecto: {len(proyecto.keywords) - keywords_availables}")
                return {
                "status": "error",
                "code": "NO_KEYWORDS_AVAILABLE",
                "message": "No tienes suficientes keywords disponibles para crear un nuevo proyecto."
                }
            else:
                print(f"Puede crear proyecto: {keywords_availables-len(proyecto.keywords)}")
                
                # Convert nested keyword arrays to objects for Firestore
                keywords_objects = []
                for kw in proyecto.keywords:
                    if isinstance(kw, list) and len(kw) >= 1:
                        kw_obj = {
                            "keyword": kw[0] if len(kw) > 0 else "",
                            "categoria": kw[1] if len(kw) > 1 else "",
                            "subcategoria": kw[2] if len(kw) > 2 else "",
                            "intencion": kw[3] if len(kw) > 3 else "",
                            "volumen": kw[4] if len(kw) > 4 else 0
                        }
                        keywords_objects.append(kw_obj)
                    elif isinstance(kw, dict):
                        keywords_objects.append(kw)
                    elif isinstance(kw, str):
                        keywords_objects.append({"keyword": kw})
                
                # Insert project into Firestore
                project_data = {
                    "nombre_proyecto": proyecto.nombre_proyecto,
                    "dominio_princpal": proyecto.dominio_princpal,
                    "subdomain_enabled": bool(proyecto.subdomain_enabled),
                    "idioma": proyecto.idioma,
                    "pais": proyecto.pais,
                    "keywords": keywords_objects,
                    "competidores": proyecto.competidores,
                    "fecha_creaciones": fecha_funcion,
                    "estado": proyecto.estado,
                    "coordenadas": proyecto.coordenadas,
                    "paid_enabled": bool(proyecto.paid_enabled),
                    "user_owner": proyecto.usuario,
                    "project_id": proyecto.project_id
                }
                
                projects_ref = get_projects_collection()
                doc_ref = projects_ref.document(proyecto.project_id)
                doc_ref.set(project_data)
                print("Inserción en Firestore completada exitosamente.")
                
                background_tasks.add_task(crearFirstProyect, proyecto.nombre_proyecto, proyecto.dominio_princpal, bool(proyecto.subdomain_enabled), proyecto.idioma, proyecto.keywords, proyecto.competidores, fecha_funcion, proyecto.coordenadas, proyecto.paid_enabled, proyecto.project_id)
                return {
                    "status": "success",
                    "code": "PROJECT_CREATED",
                    "message": "Proyecto creado exitosamente."
                }
    except Exception as e:
        print(f"ERROR: Ha ocurrido un error al crear el proyecto en el backend: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error interno del servidor: {e}")

@router.post('/actualizar/',status_code=status.HTTP_202_ACCEPTED)
async def actualizarProyecto(proyecto: Proyecto):
    try:
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_funcion = now_peru.strftime(fmt)
        keyword_count_total = 0
        keyword_count_user = 0
        
        print(f'--->> VALIDANDO LA CANTIDAD DE KEYWORDS DISPONIBLES <<---')
        
        # Get user's keywords count from Firestore projects (excluding current project)
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', proyecto.usuario).where('estado', '==', 1)
        docs = query.stream()
        
        for doc in docs:
            data = doc.to_dict()
            # Exclude current project from count
            if data.get('project_id') != proyecto.project_id:
                keywords = data.get('keywords', [])
                keyword_count_user += len(keywords)
        
        # Get user's total allowed keywords from Firestore users
        users_ref = get_users_collection()
        user_doc = users_ref.document(proyecto.usuario).get()
        if user_doc.exists:
            user_data = user_doc.to_dict()
            keyword_count_total = user_data.get('keywords_count', 0)
        
        keywords_availables = keyword_count_total - keyword_count_user
        if((keywords_availables) <= 0):
            return {
                "status": "error",
                "code": "NO_KEYWORDS_AVAILABLE",
                "message": "No tienes suficientes keywords disponibles para actualizar su proyecto."
            }
        else:
            if (keywords_availables - len(proyecto.keywords) <= 0):
                print(f"No puede actualizar proyecto: {len(proyecto.keywords) - keywords_availables}")
                return {
                "status": "error",
                "code": "NO_KEYWORDS_AVAILABLE",
                "message": "No tienes suficientes keywords disponibles para actualizar su proyecto."
                }
            else:
                print(f"Puede actualizar proyecto: {len(proyecto.keywords) - keywords_availables}")
                
                # Convert nested keyword arrays to objects for Firestore
                keywords_objects = []
                for kw in proyecto.keywords:
                    if isinstance(kw, list) and len(kw) >= 1:
                        kw_obj = {
                            "keyword": kw[0] if len(kw) > 0 else "",
                            "categoria": kw[1] if len(kw) > 1 else "",
                            "subcategoria": kw[2] if len(kw) > 2 else "",
                            "intencion": kw[3] if len(kw) > 3 else "",
                            "volumen": kw[4] if len(kw) > 4 else 0
                        }
                        keywords_objects.append(kw_obj)
                    elif isinstance(kw, dict):
                        keywords_objects.append(kw)
                    elif isinstance(kw, str):
                        keywords_objects.append({"keyword": kw})
                
                # Update project in Firestore
                projects_ref = get_projects_collection()
                # Find project by nombre_proyecto
                query = projects_ref.where('nombre_proyecto', '==', proyecto.nombre_proyecto).limit(1)
                docs = list(query.stream())
                
                if docs:
                    doc_ref = docs[0].reference
                    doc_ref.update({
                        "estado": proyecto.estado,
                        "dominio_princpal": proyecto.dominio_princpal,
                        "subdomain_enabled": bool(proyecto.subdomain_enabled),
                        "idioma": proyecto.idioma,
                        "pais": proyecto.pais,
                        "keywords": keywords_objects,
                        "competidores": proyecto.competidores,
                        "coordenadas": proyecto.coordenadas,
                        "paid_enabled": bool(proyecto.paid_enabled)
                    })
                
                return {
                    "status": "success",
                    "code": "PROJECT_UPDATED",
                    "message": "Proyecto actualizado exitosamente."
                }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get('/obtener/')
def getProyectosActivos(id: str, tag: str):
    try:
        if procesarRequest(id, tag) == True:
            return {'message':'ok'}
        else:
            return {'message':'error'}
    except:
        return {'message':'error'}

@router.get('/obtener_keywords_count/')
def getKeywordsCount(username: str):
    try:
        keywords = obtenerKeywordsTotales(username)
        print(keywords)
        return {'keywords':keywords}
    except:
        return {'message':'error'}

@router.get('/obtener-bench-seo/')
def getBenchSEO(id: str, tag: str):
    try:
        if procesarRequestBench(id, tag) == True:
            return {'message':'ok'}
        else:
            return {'message':'error'}
    except:
        return {'message':'error'}

@router.post('/actualizar_estado/')
def updateProyecto(proyecto: Proyecto):
    print(proyecto.competidores)
    try:
        # Update project in Firestore
        projects_ref = get_projects_collection()
        # Find project by nombre_proyecto
        query = projects_ref.where('nombre_proyecto', '==', proyecto.nombre_proyecto).limit(1)
        docs = list(query.stream())
        
        if docs:
            doc_ref = docs[0].reference
            doc_ref.update({
                "estado": proyecto.estado,
                "dominio_princpal": proyecto.dominio_princpal,
                "subdomain_enabled": bool(proyecto.subdomain_enabled),
                "idioma": proyecto.idioma,
                "pais": proyecto.pais,
                "competidores": proyecto.competidores,
                "coordenadas": proyecto.coordenadas,
                "paid_enabled": bool(proyecto.paid_enabled)
            })
        
        return {
                    "status": "success",
                    "code": "PROJECT_UPDATED",
                    "message": "Proyecto actualizado exitosamente."
                }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get('/listar_proyectos/')
def listarProyectos():
    try:
        projects_ref = get_projects_collection()
        query = projects_ref.where('estado', '==', 1)
        docs = query.stream()
        
        proyectos = []
        for doc in docs:
            data = doc.to_dict()
            proyectos.append({"nombre_proyecto": data.get("nombre_proyecto")})
        return proyectos
    except:
        return {'message':'error'}
    
@router.get('/listar_proyectos_angular/')
def listarProyectosAngular(username: str):
    print(username)
    try:
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', username)
        docs = query.stream()
        
        proyectos = []
        for doc in docs:
            data = doc.to_dict()
            proyectos.append({
                "nombre_proyecto": data.get("nombre_proyecto"),
                "dominio_principal": data.get("dominio_princpal"),
                "pais": data.get("pais"),
                "estado": data.get("estado")
            })
        return proyectos
    except Exception as e:
        import traceback
        print(f"Error in listarProyectosAngular: {e}")
        traceback.print_exc()
        return {'message':'error', 'detail': str(e)}
    
@router.get('/listar_proyectos_angular_desactivados/')
def listarProyectosAngularDesactivados(username: str):
    print(username)
    try:
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', username).where('estado', '==', 0)
        docs = query.stream()
        
        proyectos = []
        for doc in docs:
            data = doc.to_dict()
            proyectos.append({
                "nombre_proyecto": data.get("nombre_proyecto"),
                "dominio_principal": data.get("dominio_princpal"),
                "pais": data.get("pais")
            })
        return proyectos
    except:
        return {'message':'error'}
    
@router.get('/listar_proyectos_user/')
def listarProyectosUser(username: str):
    nombreU = urllib.parse.unquote(username)
    try:
        projects_ref = get_projects_collection()
        query = projects_ref.where('user_owner', '==', nombreU)
        docs = query.stream()
        
        proyectos = []
        for doc in docs:
            data = doc.to_dict()
            proyectos.append({"nombre_proyecto": data.get("nombre_proyecto")})
        return proyectos
        return proyectos
    except:
        return {'message':'error'}

@router.get('/listar_proyectos_desactivados/')
def listarProyectosDesactivados():
    try:
        sql = "SELECT nombre_proyecto FROM `neo-rank-tracker.rank_tracker_neo.tbl_proyecto` where estado = 0"
        df = clientbq.query(sql).to_dataframe()
        datajson = {"items":[]}
        for ind in df.index:
            nombre_proyecto = df['nombre_proyecto'][ind]
            item = {"nombre_proyecto":nombre_proyecto}
            datajson["items"].append(item)
        proyectos = datajson['items']
        return proyectos
    except:
        return {'message':'error'}

@router.get('/grafico_historico/')
def detalleHistorico(idProyect: str, dominio: str, fecha_inicio: date, fecha_fin: date):
    # Ensure dates are in YYYY-MM-DD string format or DATE objects if BQ library supports it
    # tbl_dashboard_cache.fecha is DATE type
    query = """
    SELECT 
        fecha,
        rango_grupo,
        SUM(cantidad_keywords) as cantidad_keywords
    FROM 
        `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache`
    WHERE
        id_proyecto = @idproyect AND dominio = @dominio AND tipo_resultado = 'organic' AND fecha BETWEEN @fecha_inicio AND @fecha_fin
    GROUP BY 
        fecha, 
        rango_grupo
    ORDER BY 
        fecha, 
        rango_grupo;
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("idproyect", "STRING", idProyect),
            bigquery.ScalarQueryParameter("dominio", "STRING", dominio),
            bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
            bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin)
        ]
    )
    results = clientbq.query(query, job_config=job_config).result()
    response_format = {}
    for row in results:
        # Convert date to string for JSON response
        date_str = row.fecha.strftime('%Y-%m-%d')
        if date_str not in response_format:
            response_format[date_str] = {}
        response_format[date_str][row.rango_grupo] = row.cantidad_keywords
    return response_format

@router.get('/grafico_competidores/')
def resultadosCompetidores(idProyect: str, fecha_inicio: date, fecha_fin: date):
    # Ensure dates are in YYYY-MM-DD string format or DATE objects if BQ library supports it
    query = """
    SELECT
        fecha,
        dominio,
        rango_grupo,
        SUM(cantidad_keywords) as cantidad_keywords
    FROM 
        `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache`
    WHERE
        id_proyecto = @idproyect AND tipo_resultado = 'organic' AND fecha BETWEEN @fecha_inicio AND @fecha_fin
    GROUP BY 
        fecha, 
        dominio,
        rango_grupo
    ORDER BY 
        fecha, 
        dominio,
        rango_grupo;
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("idproyect", "STRING", idProyect),
            bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio),
            bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin)
        ]
    )
    results = clientbq.query(query, job_config=job_config).result()
    formatted_result = {}

    for row in results:
        # Access via attribute and convert date
        fecha = row.fecha.strftime('%Y-%m-%d')
        dominio = row.dominio
        rango_grupo = row.rango_grupo
        cantidad_keywords = row.cantidad_keywords

        if fecha not in formatted_result:
            formatted_result[fecha] = {}

        if dominio not in formatted_result[fecha]:
            formatted_result[fecha][dominio] = {}

        formatted_result[fecha][dominio][rango_grupo] = cantidad_keywords
    return formatted_result

@router.get('/detalle_proyecto/')
def detalleProyecto(nombreP: str):
    nombreP = urllib.parse.unquote(nombreP)
    print(nombreP)
    try:
        # Optimization: Query tbl_dashboard_cache for the latest available date for this project
        # This replaces the logic of "last 7 days" aggregation with "latest snapshot"
        sql = """
        SELECT 
            dominio, 
            rango_grupo, 
            SUM(cantidad_keywords) AS cantidad_keywords 
        FROM `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache` 
        WHERE 
            id_proyecto = @nombreP 
            AND tipo_resultado = 'organic'
            AND fecha = (
                SELECT MAX(fecha) 
                FROM `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache` 
                WHERE id_proyecto = @nombreP
            )
        GROUP BY dominio, rango_grupo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("nombreP", "STRING", nombreP)
            ]
        )
        
        results = clientbq.query(sql, job_config=job_config).result()
        datajson = {"items":[]}
        
        for row in results:
            dominio = row.dominio
            rango_grupo = row.rango_grupo
            cantidad_keywords = int(row.cantidad_keywords)
            item = {"dominio":dominio, "rango_grupo":rango_grupo, "cantidad_keywords":cantidad_keywords}
            datajson["items"].append(item)
            
        proyectos = datajson['items']
        return proyectos
    except HTTPException as error:
        return {'message':error}

@router.get('/detalle_proyecto_paid/')
def detalleProyectoPaid(nombreP: str):
    nombreP = urllib.parse.unquote(nombreP)
    print(nombreP)
    try:
        # Optimization: Query tbl_dashboard_cache for the latest available date for this project
        # Includes both 'organic' and 'paid' results
        sql = """
        SELECT 
            dominio, 
            rango_grupo, 
            SUM(cantidad_keywords) AS cantidad_keywords 
        FROM `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache` 
        WHERE 
            id_proyecto = @nombreP 
            AND tipo_resultado IN ('organic', 'paid')
            AND fecha = (
                SELECT MAX(fecha) 
                FROM `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache` 
                WHERE id_proyecto = @nombreP
            )
        GROUP BY dominio, rango_grupo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("nombreP", "STRING", nombreP)
            ]
        )
        
        results = clientbq.query(sql, job_config=job_config).result()
        datajson = {"items":[]}
        
        for row in results:
            dominio = row.dominio
            rango_grupo = row.rango_grupo
            cantidad_keywords = int(row.cantidad_keywords)
            item = {"dominio":dominio, "rango_grupo":rango_grupo, "cantidad_keywords":cantidad_keywords}
            datajson["items"].append(item)
            
        proyectos = datajson['items']
        return proyectos
    except HTTPException as error:
        return {'message':error}

@router.get("/descargar_keywords/",status_code=status.HTTP_202_ACCEPTED)
async def get_project_keywords(nombre_proyecto: str):
    try:
        nombreP = urllib.parse.unquote(nombre_proyecto)
        print(nombreP)
        
        projects_ref = get_projects_collection()
        query = projects_ref.where('nombre_proyecto', '==', nombreP).limit(1)
        docs = list(query.stream())
        
        if not docs:
            raise HTTPException(status_code=404, detail="Proyecto no encontrado")
        
        row = docs[0].to_dict()
        # Ensure keywords are list
        keywords = row.get('keywords', [])
        if isinstance(keywords, str):
            keywords = json.loads(keywords)
            
        return keywords
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get('/detalle_proyecto_angular/')
def detalleProyectoAngular(nombreP: str):
    nombreP = urllib.parse.unquote(nombreP)
    print(nombreP)
    try:
        # Get project from Firestore
        projects_ref = get_projects_collection()
        query = projects_ref.where('nombre_proyecto', '==', nombreP).limit(1)
        docs = list(query.stream())
        
        if not docs:
            return {'message': 'Proyecto no encontrado'}
            
        data = docs[0].to_dict()
        
        item = {
            "nombre_proyecto": data.get("nombre_proyecto"),
            "dominio_princpal": data.get("dominio_princpal"),
            "subdomain_enabled": bool(data.get("subdomain_enabled")),
            "idioma": data.get("idioma"),
            "pais": data.get("pais"),
            "competidores": data.get("competidores", []),
            "keywords": data.get("keywords", []),
            "estado": int(data.get("estado")),
            "paid_enabled": bool(data.get("paid_enabled"))
        }

        return item
    except HTTPException as error:
        return {'message':error}

@router.get('/detalle_proyecto_keywords/')
def detalleProyectoKeywords(nombreP: str):
    nombreP = urllib.parse.unquote(nombreP)
    print(nombreP)
    try:
        sql = f"SELECT keyword, categoria, subcategoria, intencion, volumen, dominio, posicion_absoluto, rango_absoluto FROM `neo-rank-tracker.rank_tracker_neo.tbl_keywords_organic_final` WHERE fecha >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) and id_proyecto = '{nombreP}' and not url = 'no posiciona'"
        df = clientbq.query(sql).to_dataframe()
        datajson = {"items":[]}
        for ind in df.index:
            keyword = df['keyword'][ind]
            categoria = df['categoria'][ind]
            subcategoria = df['subcategoria'][ind]
            intencion = df['intencion'][ind]
            volumen = int(df['volumen'][ind])
            dominio = df['dominio'][ind]
            posicion_absoluto = int(df['posicion_absoluto'][ind])
            rango_absoluto = df['rango_absoluto'][ind]
            item = {"keyword":keyword, "categoria":categoria, "subcategoria":subcategoria, "intencion":intencion, "volumen": volumen, "dominio":dominio, "posicion_absoluto":posicion_absoluto, "rango_absoluto":rango_absoluto}
            datajson["items"].append(item)
        proyectos = datajson['items']
        return proyectos
    except HTTPException as error:
        return {'message':error}

@router.get('/comparador_semanal/')
async def comparador(background_tasks: BackgroundTasks):
    try:
        sql = "SELECT DISTINCT nombre_proyecto, dominio_principal FROM  `neo-rank-tracker.rank_tracker_neo.tbl_keywords_pre_request` WHERE  PARSE_DATE('%Y-%m-%d', fecha) = DATE_SUB(CURRENT_DATE('America/Bogota'), INTERVAL 1 DAY)"
        df = clientbq.query(sql).to_dataframe()
        contador = 1
        cantidad = len(df.index)
        datajson = {"items":[]}
        for ind in df.index:
            nombre_proyecto = df['nombre_proyecto'][ind]
            dominio_principal = df['dominio_principal'][ind]
            item = {"nombre_proyecto":nombre_proyecto, "dominio_principal":dominio_principal}
            datajson["items"].append(item)
        proyectos = datajson['items']
        for proy in proyectos:
            print(f'--->> COMPARADOR SEMANAL INICIADO <<--- {contador} | {cantidad} | ' + proy['nombre_proyecto'])
            background_tasks.add_task(comparadorSemanal, proy['nombre_proyecto'], proy['dominio_principal'])
            contador += 1
        print(f'--->> COMPARADOR SEMANAL TERMINADO <<---')
        return {'message':'ok'}
    except HTTPException as error:
        return {'message':error}
    

def send_keyword_request(data):
    try:
        post_data = dict()
        post_data[len(post_data)] = dict(
            priority=1,
            language_name=data['idioma'],
            location_coordinate=data['coordenadas'],
            se_domain="google.com",
            tag=urllib.parse.quote(data['nombre_proyecto']),
            depth=30,
            keyword="{}".format(urllib.parse.quote(data['keyword'][0])),
            pingback_url="https://rank-tracker-process-v2-iglbh7yflq-uc.a.run.app/rank_process/obtener/?id=$id&tag=$tag"
        )
        response = client.post("/v3/serp/google/organic/task_post", post_data)
        if response["status_code"] == 20000:
            task_id = response["tasks"][0]["id"]
            dataset_id = 'rank_tracker_neo'
            table_id = 'tbl_keywords_pre_request'
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)
            rows_to_insert = [
                (
                    data['nombre_proyecto'],
                    data['keyword'][0],
                    data['keyword'][1],
                    data['keyword'][2],
                    data['keyword'][3],
                    data['keyword'][4],
                    task_id,
                    data['fecha'],
                    data['subdomains'],
                    json.dumps(data['dominios']),
                    data['dominioP'],
                    data['paid_results']
                )
            ]
            errors = clientbq.insert_rows(table, rows_to_insert)
            if errors == []:
                return {"message": "ok"}
            else:
                return {"message": errors}
        else:
            return {"message": "error. Code: %d Message: %s" % (response["status_code"], response["status_message"])}
    except Exception as error:
        return {"message": f"Ha ocurrido un error: {error}"}
        
def crearFirstProyect(nombreP:str, dominioP:str, subdomains: bool, idioma:str, keywords:list, dominios:list, fecha:str, coordenadas:str, paid_results:bool, project_id: str):
    # Validar si keywords es una lista de listas, de lo contrario intentar parsearlo como JSON
    if not isinstance(keywords, list) or not all(isinstance(item, list) and len(item) == 5 for item in keywords):
        try:
            if isinstance(keywords, str):
                keywords = json.loads(keywords)
            if not isinstance(keywords, list) or not all(isinstance(item, list) and len(item) == 5 for item in keywords):
                raise ValueError("Formato de keywords incorrecto después de decodificar JSON")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error al decodificar keywords: {e}")
            return  # Detiene la ejecución si no se puede decodificar correctamente
    for keyword in keywords:
        print(keyword)
        post_data = dict()
        post_data[len(post_data)] = dict(
            priority=1,
            language_name = idioma,
            location_coordinate = coordenadas,
            se_domain = "google.com",
            tag=project_id,
            depth = 30,
            max_crawl_pages = 4,
            search_param = "adtest=on",
            keyword="{}".format(urllib.parse.quote(keyword[0])),
            pingback_url="https://rank-tracker-firestore-448577132.us-central1.run.app/rank_tracker/obtener/?id=$id&tag=$tag"
            )
        response = client.post("/v3/serp/google/organic/task_post", post_data)
        if response["status_code"] == 20000:
            task_id = response["tasks"][0]["id"]
            table_id = 'tbl_keywords_pre_request'
            dataset_id = 'rank_tracker_neo'
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)
            rows_to_insert = [(nombreP, keyword[0], keyword[1], keyword[2], keyword[3], keyword[4],task_id,fecha, subdomains,dominios,dominioP, paid_results, project_id)]
            errors = clientbq.insert_rows(table, rows_to_insert)
            if errors == []:
                print('Data insertada correctamente')
            else:
                print(errors)
        else:
            print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
def crearBenchProyect(nombreP:str, idioma:str, pais:str, coordenadas:str, keywords: Union[list, str], paid_results:bool, fecha:str):
    # keywords puede ser una lista Python o un JSON string; manejar ambos casos
    if isinstance(keywords, str):
        try:
            jkeywords = json.loads(keywords)
        except json.JSONDecodeError:
            print("Error al decodificar keywords JSON; se esperaba un JSON válido.")
            return
    else:
        jkeywords = keywords

    for keyword in jkeywords:
        post_data = dict()
        post_data[len(post_data)] = dict(
            priority=1,
            language_name = idioma,
            location_coordinate = coordenadas,
            se_domain = "google.com",
            tag=urllib.parse.quote(nombreP),
            depth = 20,
            keyword="{}".format(quote(keyword['Keyword'])),
            pingback_url="https://rank-tracker-firestore-448577132.us-central1.run.app/rank_tracker/obtener-bench-seo/?id=$id&tag=$tag"
            )
        response = client.post("/v3/serp/google/organic/task_post", post_data)
        if response["status_code"] == 20000:
            task_id = response["tasks"][0]["id"]
            dataset_id = 'rank_tracker_bench_seo'
            table_id = 'tbl_bench_seo_pre_request'
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)
            rows_to_insert = [(nombreP, keyword['Keyword'], keyword['Categoría'], keyword['Subcategoría'], keyword['Intención'], keyword['Volumen'], paid_results, task_id, fecha)]
            errors = clientbq.insert_rows(table, rows_to_insert)
            if errors == []:
                print('Data insertada correctamente')
            else:
                print(errors)
        else:
            print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))


def procesarRequestBench(id: str, tag: str):
    try:
        print(f'--->> OBTENCION RESULTADOS BENCHMARK <<---')
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_funcion = now_peru.strftime(fmt)
        idproyect = urllib.parse.unquote(tag)
        query = f"SELECT nombre_proyecto, keyword, categoria, subcategoria, intencion, volumen, paid_enabled FROM `neo-rank-tracker.rank_tracker_bench_seo.tbl_bench_seo_pre_request` WHERE taskId = @taskId and nombre_proyecto = @nombre_proyecto"
        query_params = [
            bigquery.ScalarQueryParameter("taskId", "STRING", id),
            bigquery.ScalarQueryParameter("nombre_proyecto", "STRING", idproyect)]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = clientbq.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        datajson = {"items":[]}
        for ind in df.index:
            nombre_proyecto = df['nombre_proyecto'][ind]
            keyword = df['keyword'][ind]
            categoria = df['categoria'][ind]
            subcategoria = df['subcategoria'][ind]
            intencion = df['intencion'][ind]
            volumen = int(df['volumen'][ind])
            paid_enabled = bool(df['paid_enabled'][ind])
            item = {"nombre_proyecto":nombre_proyecto, "keyword":keyword, "categoria": categoria, "subcategoria":subcategoria, "intencion":intencion, "volumen":volumen, "paid_enabled": paid_enabled}
            datajson["items"].append(item)
        proyectos = datajson['items']
        dataset_id = 'rank_tracker_bench_seo'
        table_id = 'tbl_bench_seo_final'
        table_ref = clientbq.dataset(dataset_id).table(table_id)
        table = clientbq.get_table(table_ref)
        for proy in proyectos:
            results = client.get("/v3/serp/google/organic/task_get/advanced/{}".format(id))
            if results["tasks_error"] != 0:
                print("error. Code: %d Message: %s" % (results["status_code"], results["status_message"]))
                return "error"
            else:
                keyword_rank = results["tasks"][0]['data']['keyword']
                items = results["tasks"][0]['result'][0]["items"]
                if proy['paid_enabled'] == True:
                    for item in items[:8]:
                        if(item['type'] in ('paid', 'organic')):
                            tipo_resultado = item['type']
                            position_group = item['rank_group']
                            position_absolute = item['rank_absolute']
                            domain_result = item['domain']
                            url_rank = item['url']
                            if(item['title'] == None):
                                title = "No meta-title"
                            else:
                                title = item['title'].replace(u'\xa0',u' ')
                            if(item['description'] == None):
                                description = "No meta-description"
                            else:
                                description = item['description'].replace(u'\xa0',u' ')
                            if(item['breadcrumb'] == None):
                                breadcrumbs = "No breadcrumbs"
                            else:
                                breadcrumbs = item['breadcrumb'].replace(u'\xa0',u' ')
                            if 1 <= position_group < 4:
                                rango_group = "1) Pos 1-3"
                            elif 4 <= position_group < 11:
                                rango_group = "2) Pos 4-10"
                            elif 11 <= position_group < 21:
                                rango_group = "3) Pos 11-20"
                            else:
                                rango_group = "4) Pos 20+"
                            if 1 <= position_absolute < 4:
                                rango_absolute = "1) Pos 1-3"
                            elif 4 <= position_absolute < 11:
                                rango_absolute = "2) Pos 4-10"
                            elif 11 <= position_absolute < 21:
                                rango_absolute = "3) Pos 11-20"
                            else:
                                rango_absolute = "4) Pos 20+"
                            rows_to_insert = [(proy['nombre_proyecto'], proy['keyword'], proy['categoria'], proy['subcategoria'], proy['intencion'], proy['volumen'], id, fecha_funcion, domain_result, position_group, rango_group, position_absolute, rango_absolute, url_rank, breadcrumbs, tipo_resultado, title, description)]
                            print(f'-->> Insertando>: {rows_to_insert}')
                            errors = clientbq.insert_rows(table, rows_to_insert)
                            if errors == []:
                                print('Data insertada correctamente')
                            else:
                                print(errors)
                else:
                    for item in items[:8]:
                        if(item['type'] == 'organic'):
                            tipo_resultado = item['type']
                            position_group = item['rank_group']
                            position_absolute = item['rank_absolute']
                            domain_result = item['domain']
                            url_rank = item['url']
                            if(item['title'] == None):
                                title = "No meta-title"
                            else:
                                title = item['title'].replace(u'\xa0',u' ')
                            if(item['description'] == None):
                                description = "No meta-description"
                            else:
                                description = item['description'].replace(u'\xa0',u' ')
                            if(item['breadcrumb'] == None):
                                breadcrumbs = "No breadcrumbs"
                            else:
                                breadcrumbs = item['breadcrumb'].replace(u'\xa0',u' ')
                            if 1 <= position_group < 4:
                                rango_group = "1) Pos 1-3"
                            elif 4 <= position_group < 11:
                                rango_group = "2) Pos 4-10"
                            elif 11 <= position_group < 21:
                                rango_group = "3) Pos 11-20"
                            else:
                                rango_group = "4) Pos 20+"
                            if 1 <= position_absolute < 4:
                                rango_absolute = "1) Pos 1-3"
                            elif 4 <= position_absolute < 11:
                                rango_absolute = "2) Pos 4-10"
                            elif 11 <= position_absolute < 21:
                                rango_absolute = "3) Pos 11-20"
                            else:
                                rango_absolute = "4) Pos 20+"
                            rows_to_insert = [(proy['nombre_proyecto'], proy['keyword'], proy['categoria'], proy['subcategoria'], proy['intencion'], proy['volumen'], id, fecha_funcion, domain_result, position_group, rango_group, position_absolute, rango_absolute, url_rank, breadcrumbs, tipo_resultado, title, description)]
                            print(f'-->> Insertando>: {rows_to_insert}')
                            errors = clientbq.insert_rows(table, rows_to_insert)
                            if errors == []:
                                print('Data insertada correctamente')
                            else:
                                print(errors)
                        else:
                            pass
        print(f'--->> OBTENCION BENCHMARK POR TASKID TERMINADO <<---')
        return True
    except Exception as error:
        print(error)
        return False

def procesarRequest(id: str, tag: str):
    nowutc = datetime.now(timezone('GMT'))
    now_peru = nowutc.astimezone(timezone('America/Bogota'))
    fecha_funcion = now_peru.strftime("%Y-%m-%d")
    clientbq = bigquery.Client()

    try:
        print(f'--->> OBTENCION RESULTADOS POR TASKID INICIADO <<---')
        idproyect = urllib.parse.unquote(tag)
        print(f'Parametros: taskId={id}, project_id={idproyect}')

        proyecto = obtener_proyectos(id, idproyect)
        print(f'Proyectos obtenidos: {proyecto}')

        dataset_id = 'rank_tracker_neo'
        table_id = 'tbl_keywords_organic_final'
        table_ref = clientbq.dataset(dataset_id).table(table_id)
        table = clientbq.get_table(table_ref)
        rows_to_insert = []

        jdominios = proyecto['dominios']
        if isinstance(jdominios, str):
            try:
                jdominios = json.loads(jdominios)
            except json.JSONDecodeError:
                print("Error al decodificar JSON. jdominios_str no es un JSON válido.")
                return False

        results = client.get(f"/v3/serp/google/organic/task_get/advanced/{id}")
        if results["tasks_error"] != 0:
            print("Error. Code: %d Message: %s" % (results["status_code"], results["status_message"]))
            return False

        keyword_rank = results["tasks"][0]['data']['keyword']
        items = results["tasks"][0]['result'][0]["items"]

        if proyecto['paid_enabled']:
            for item in items[:4]:
                if item['type'] == 'paid':
                    procesar_item(proyecto, item, id, fecha_funcion, "paid", rows_to_insert)

        if proyecto['subdomains_enabled']:
            jdominios.append(proyecto['dominio_principal'])
            for dominio in jdominios:
                dominio_search = obtener_dominio_principal(dominio)
                encontrado = False
                for item in items:
                    if item['type'] == 'organic' and dominio_search in item['domain']:
                        procesar_item(proyecto, item, id, fecha_funcion, "organic", rows_to_insert)
                        encontrado = True
                        break
                if not encontrado:
                    agregar_no_posiciona(proyecto, dominio, id, fecha_funcion, rows_to_insert)
        else:
            jdominios.append(proyecto['dominio_principal'])
            for dominio in jdominios:
                encontrado = False
                for item in items:
                    if item['type'] == 'organic' and dominio == item['domain']:
                        procesar_item(proyecto, item, id, fecha_funcion, "organic", rows_to_insert)
                        encontrado = True
                        break
                if not encontrado:
                    agregar_no_posiciona(proyecto, dominio, id, fecha_funcion, rows_to_insert)

        if rows_to_insert:
            print(f'-->> Insertando {len(rows_to_insert)} registros')
            errors = clientbq.insert_rows(table, rows_to_insert)
            if errors == []:
                print('Data insertada correctamente')
            else:
                print(f'Errores al insertar: {errors}')
        else:
            print('No hay datos para insertar.')

        print(f'--->> OBTENCION RESULTADOS POR TASKID TERMINADO <<---')
        return True
    except Exception as e:
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return False

def procesar_item(proyecto, item, task_id, fecha_funcion, tipo_resultado, rows_to_insert):
    nombre_proyecto = proyecto['nombre_proyecto']
    project_id = proyecto['project_id']
    keyword = proyecto['keyword']
    categoria = proyecto['categoria']
    subcategoria = proyecto['subcategoria']
    intencion = proyecto['intencion']
    volumen = proyecto['volumen']
    domain_result = item['domain']
    position_group = item['rank_group']
    position_absolute = item['rank_absolute']
    url_rank = item['url']
    title = item['title'] if item['title'] else "No meta-title"
    description = item['description'] if item['description'] else "No meta-description"
    breadcrumbs = item['breadcrumb'] if item['breadcrumb'] else "No breadcrumbs"

    rango_group = obtener_rango(position_group)
    rango_absolute = obtener_rango(position_absolute)

    rows_to_insert.append((
        nombre_proyecto, keyword, categoria, subcategoria, intencion, volumen, task_id,
        fecha_funcion, domain_result, position_group, rango_group, position_absolute,
        rango_absolute, url_rank, breadcrumbs, tipo_resultado, title, description, project_id
    ))

def obtener_rango(posicion):
    if 1 <= posicion < 4:
        return "1) Pos 1-3"
    elif 4 <= posicion < 11:
        return "2) Pos 4-10"
    elif 11 <= posicion < 21:
        return "3) Pos 11-20"
    else:
        return "4) Pos 20+"

def agregar_no_posiciona(proyecto, dominio, task_id, fecha_funcion, rows_to_insert):
    rows_to_insert.append((
        proyecto["nombre_proyecto"], proyecto["keyword"], proyecto["categoria"], proyecto["subcategoria"],
        proyecto["intencion"], proyecto["volumen"], task_id, fecha_funcion, dominio, 0,
        "no posiciona", 0, "no posiciona", "no posiciona", "no posiciona", "no posiciona",
        "no posiciona", "no posiciona", proyecto["project_id"]
    ))

def agregarProyectoSemanal(nombreP:str, dominioP:str, subdomains: bool, idioma:str, keywords:list, dominios:list, fecha:str, coordenadas:str, paid_results:str, project_id: str):
    try:
        for keyword in keywords:
            post_data = dict()
            post_data[len(post_data)] = dict(
                priority=1,
                language_name = idioma,
                location_coordinate = coordenadas,
                se_domain = "google.com",
                tag=project_id,
                depth = 30,
                keyword = "{}".format(urllib.parse.quote(keyword[0])),
                pingback_url="https://rank-tracker-process-448577132.us-central1.run.app/rank_process/obtener/?id=$id&tag=$tag"
            )
            response = client.post("/v3/serp/google/organic/task_post", post_data)
            if response["status_code"] == 20000:
                task_id = response["tasks"][0]["id"]
                dataset_id = 'rank_tracker_neo'
                table_id = 'tbl_keywords_pre_request'
                table_ref = clientbq.dataset(dataset_id).table(table_id)
                table = clientbq.get_table(table_ref)
                rows_to_insert = [(nombreP, keyword[0], keyword[1], keyword[2], keyword[3], keyword[4], task_id, fecha, subdomains, json.dumps(dominios), dominioP, paid_results, project_id)]
                print(f'-->> Insertando task keyword: {rows_to_insert}')
                errors = clientbq.insert_rows(table, rows_to_insert)
                if errors == []:
                    print('Data insertada correctamente')
                else:
                    print(errors)
            else:
                print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Ha ocurrido un error: {error}")

async def agregar_proyecto_semanal(nombreP, dominioP, subdomains, idioma, keywords, dominios, fecha, coordenadas, paid_results):
    username = os.environ.get('DATAFORSEO_USERNAME')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    if not username or not password:
        raise HTTPException(status_code=500, detail="DataForSEO credentials not configured")
    async with httpx.AsyncClient(auth=(username, password)) as client:
        for keyword in keywords:
            post_data = [{
                "priority": 1,
                "language_name": idioma,
                "location_coordinate": coordenadas,
                "se_domain": "google.com",
                "tag": urllib.parse.quote(nombreP),
                "depth": 30,
                "keyword": urllib.parse.quote(keyword[0]),  # Asumiendo que keyword[0] es el término de búsqueda
                "pingback_url": "https://rank-tracker-process-v2-iglbh7yflq-uc.a.run.app/rank_process/obtener/?id=$id&tag=$tag"
            }]
            
            try:
                response = await client.post("https://api.dataforseo.com/v3/serp/google/organic/task_post", json=post_data)
                response_data = response.json()
                if response.status_code == 200 and response_data["status_code"] == 20000:
                    task_id = response_data["tasks"][0]["id"]
                    dataset_id = 'rank_tracker_neo'
                    table_id = 'tbl_keywords_pre_request'
                    table_ref = clientbq.dataset(dataset_id).table(table_id)
                    table = clientbq.get_table(table_ref)
                    rows_to_insert = [(nombreP, keyword[0], keyword[1], keyword[2], keyword[3], keyword[4], task_id, fecha, subdomains, json.dumps(dominios), dominioP, paid_results)]
                    print(f'-->> Insertando task keyword: {rows_to_insert}')
                    errors = clientbq.insert_rows(table, rows_to_insert)
                    if errors == []:
                        print('Data insertada correctamente')
                    else:
                        print(errors)
                    print(f'Task creado con ID: {task_id}')
                else:
                    print(f"Error al publicar tarea: {response_data['status_message']}")
            except Exception as error:
                print(f"Error al realizar la solicitud: {error}")

def runMissingTasks(fecha: str):
    dataset_id = 'rank_tracker_neo'
    table_id = 'tbl_keywords_pre_request'
    table_ref = clientbq.dataset(dataset_id).table(table_id)
    table = clientbq.get_table(table_ref)

    sql = f"""
            SELECT nombre_proyecto, taskId 
            FROM `neo-rank-tracker.rank_tracker_neo.tbl_keywords_pre_request` 
            WHERE fecha = '{fecha}';
          """
    print(sql)
    # Ejecutar la consulta en BigQuery
    query_job = clientbq.query(sql)
    results = query_job.result()
    
    for row in results:
        nombre_proyecto, taskId = row.nombre_proyecto, row.taskId
        url = f"https://rank-tracker-process-iglbh7yflq-uc.a.run.app/rank_process/obtener/?id={taskId}&tag={urllib.parse.quote(nombre_proyecto)}"
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Enviado correctamente: {nombre_proyecto}, {taskId}")
        else:
            print(f"Error al enviar: {nombre_proyecto}, {taskId}")
    

def obtenerKeywordsTotales(username: str):
    sql =   f"""
                WITH Keywords AS (
                SELECT
                    user_owner,
                    estado,
                    JSON_EXTRACT_SCALAR(keyword_data, '$[0]') AS keyword
                FROM
                    `neo-rank-tracker.rank_tracker_neo.tbl_proyecto`,
                    UNNEST(JSON_EXTRACT_ARRAY(keywords)) AS keyword_data
                )
                SELECT
                COUNT(DISTINCT keyword) AS keyword_count
                FROM
                Keywords
                WHERE
                user_owner = '{username}' and estado = 1
            """
    query_job = clientbq.query(sql)
    results = query_job.result()
    if results:
        for row in results:
            return 500 - row.keyword_count
    else:
        return 500


def comparadorSemanal(nproyecto: str, dominio: str):
    sql =   f"""
            WITH lunes_actuales AS (
                SELECT 
                    Keyword,
                    categoria,
                    subcategoria,
                    intencion,
                    volumen,
                    url,
                    posicion_grupo AS posicion_actual,
                    fecha
                FROM 
                    `neo-rank-tracker.rank_tracker_neo.tbl_keywords_organic_final`
                WHERE 
                    PARSE_DATE('%Y-%m-%d', fecha) = DATE_SUB(CURRENT_DATE('America/Bogota'), INTERVAL 1 DAY)
                    AND id_proyecto = '{nproyecto}' AND dominio = '{dominio}' and tipo_resultado = 'organic'
                ),
                lunes_anteriores AS (
                SELECT 
                    Keyword,
                    categoria,
                    subcategoria,
                    intencion,
                    volumen,
                    url,
                    posicion_grupo AS posicion_anterior,
                    fecha
                FROM 
                    `neo-rank-tracker.rank_tracker_neo.tbl_keywords_organic_final`
                WHERE 
                    PARSE_DATE('%Y-%m-%d', fecha) = DATE_SUB(CURRENT_DATE('America/Bogota'), INTERVAL 8 DAY)
                    AND id_proyecto = '{nproyecto}' AND dominio = '{dominio}' and tipo_resultado = 'organic'
                )
                SELECT 
                    a.keyword,
                    a.categoria,
                    a.subcategoria,
                    a.intencion,
                    a.volumen,
                    a.url,
                    a.posicion_actual,
                    b.posicion_anterior,
                    a.fecha,
                    CASE 
                        WHEN a.posicion_actual < b.posicion_anterior THEN 'Subió'
                        WHEN a.posicion_actual > b.posicion_anterior THEN 'Bajó'
                        ELSE 'Se mantuvo'
                    END AS comparacion
                    FROM 
                    lunes_actuales a
                    JOIN 
                    lunes_anteriores b
                    ON 
                    a.keyword = b.Keyword;
            """
    df = clientbq.query(sql).to_dataframe()
     # Verifica si el DataFrame df está vacío
    if df.empty:
        print('Sin comparación semanal para proyecto:', nproyecto, 'y dominio:', dominio)
        return  # Termina la función aquí, no hay nada que insertar
    datajson = {"items":[]}
    for ind in df.index:
        keyword = df['keyword'][ind]
        categoria = df['categoria'][ind]
        subcategoria = df['subcategoria'][ind]
        intencion = df['intencion'][ind]
        volumen = int(df['volumen'][ind])
        url = df['url'][ind]
        posicion_actual = int(df['posicion_actual'][ind])
        posicion_anterior = int(df['posicion_anterior'][ind])
        comparacion = df['comparacion'][ind]
        fecha = df['fecha'][ind]
        item = {"nombre_proyecto":nproyecto, "keyword":keyword, "categoria":categoria, "subcategoria":subcategoria, "intencion":intencion, "volumen":volumen, "url":url, "posicion_actual":posicion_actual, "posicion_anterior":posicion_anterior, "comparacion":comparacion, "fecha":fecha}
        datajson["items"].append(item)
    rows_to_insert = [
        (
            proy["nombre_proyecto"], proy["keyword"], proy["categoria"], proy["subcategoria"],
            proy["intencion"], proy["volumen"], proy["url"], proy["posicion_actual"],
            proy["posicion_anterior"], proy["comparacion"], proy["fecha"]
        ) for proy in datajson["items"]
    ]
    # Realizamos una única llamada a insert_rows con todas las filas preparadas.
    print(f'-->> Insertando comparaciones: {len(rows_to_insert)} registros')
    dataset_id = 'rank_tracker_neo'
    table_id = 'tbl_comparador_rank_tracker'
    table_ref = clientbq.dataset(dataset_id).table(table_id)
    table = clientbq.get_table(table_ref)
    errors = clientbq.insert_rows(table, rows_to_insert) # Asegúrate de que 'table' está definido como antes.
    if errors == []:
        print('Data insertada correctamente')
    else:
        print(errors)
   
def obtener_dominio_principal(url):
    extracted = tldextract.extract(url)
    return extracted.domain

@router.post('/audit_seo/crear', status_code=status.HTTP_202_ACCEPTED)
async def crear_proyecto_audit_seo(project: AuditSEOCreateRequest, background_tasks: BackgroundTasks):
    """
    Crea un nuevo proyecto de Auditoría SEO y envía una tarea a DataForSEO.
    """
    try:
        print(f'--->> CREACIÓN DE PROYECTO AUDITORÍA SEO INICIADO <<---')
        nowutc = datetime.now(timezone('GMT'))
        now_peru = nowutc.astimezone(timezone('America/Bogota'))
        fecha_creacion = now_peru.strftime(fmt)

        # Generar un project_id único para el proyecto de auditoría SEO
        project_id = f"audit_seo_{project.nombre_cliente}_{now_peru.strftime('%Y%m%d%H%M%S')}"

        # Preparar los datos para la API de DataForSEO On-Page
        post_data = [
            {
                "target": project.dominio_web,
                "max_crawl_pages": project.max_crawl_pages,
                "load_resources": project.render_web,  # true si render_web es true
                "enable_javascript": project.render_web, # true si render_web es true
                "tag": project_id, # Usar el project_id como tag
                "pingback_url": "https://your-domain.com/audit_seo/callback" # TODO: Reemplazar con tu URL de callback real
            }
        ]

        if project.has_sitemap and project.sitemap_url:
            post_data[0]["sitemap_url"] = project.sitemap_url

        # Enviar la tarea a DataForSEO
        response = client.post("/v3/on_page/task_post", post_data)

        if response["status_code"] == 20000:
            task_id_dataforseo = response["tasks"][0]["id"]
            print(f"Tarea DataForSEO creada con ID: {task_id_dataforseo}")

            # Guardar los detalles del proyecto en BigQuery
            dataset_id = 'rank_tracker_neo' # Reemplazar con tu dataset de BigQuery
            table_id = 'tbl_audit_seo_proyectos' # Reemplazar con tu tabla de BigQuery
            table_ref = clientbq.dataset(dataset_id).table(table_id)
            table = clientbq.get_table(table_ref)

            rows_to_insert = [(
                project.nombre_cliente,
                project.dominio_web,
                project.render_web,
                project.tracking_semanal,
                project.has_sitemap,
                project.sitemap_url,
                project.max_crawl_pages,
                project.usuario,
                project_id, # Usar el project_id generado
                task_id_dataforseo, # Guardar el task_id de DataForSEO
                fecha_creacion
            )]

            errors = clientbq.insert_rows(table, rows_to_insert)
            if errors == []:
                print('Proyecto de Auditoría SEO guardado en BigQuery')
                return {
                    "status": "success",
                    "code": "AUDIT_PROJECT_CREATED",
                    "message": "Proyecto de Auditoría SEO creado exitosamente.",
                    "project_id": project_id
                }
            else:
                print(f"Errores al insertar en BigQuery: {errors}")
                raise HTTPException(status_code=500, detail="Error al guardar el proyecto en la base de datos.")
        else:
            print(f"Error de DataForSEO: {response['status_code']} - {response['status_message']}")
            raise HTTPException(status_code=400, detail=f"Error al iniciar la auditoría con DataForSEO: {response['status_message']}")

    except Exception as e:
        print(f"Error general al crear proyecto de Auditoría SEO: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ha ocurrido un error inesperado: {str(e)}")

@router.get('/audit_seo/listar', status_code=status.HTTP_200_OK)
def listar_proyectos_audit_seo(username: str):
    """
    Lista todos los proyectos de Auditoría SEO para un usuario dado.
    """
    try:
        sql = f"""
        SELECT 
            nombre_cliente, 
            dominio_web, 
            render_web, 
            tracking_semanal, 
            max_crawl_pages, 
            usuario, 
            project_id, 
            fecha_creacion
        FROM `neo-rank-tracker.rank_tracker_neo.tbl_audit_seo_proyectos` 
        WHERE usuario = '{username}'
        """
        print(f"Ejecutando consulta BigQuery: {sql}")
        df = clientbq.query(sql).to_dataframe()
        
        proyectos = []
        for ind in df.index:
            proyecto = AuditSEOProject(
                nombre_cliente=df['nombre_cliente'][ind],
                dominio_web=df['dominio_web'][ind],
                render_web=bool(df['render_web'][ind]),
                tracking_semanal=bool(df['tracking_semanal'][ind]),
                has_sitemap=bool(df['has_sitemap'][ind]) if 'has_sitemap' in df.columns else False,
                sitemap_url=df['sitemap_url'][ind] if 'sitemap_url' in df.columns else None,
                max_crawl_pages=int(df['max_crawl_pages'][ind]),
                usuario=df['usuario'][ind],
                project_id=df['project_id'][ind],
                fecha_creacion=df['fecha_creacion'][ind]
            )
            proyectos.append(proyecto)
        
        print(f"Proyectos de Auditoría SEO encontrados: {len(proyectos)}")
        return proyectos
    except Exception as e:
        print(f"Error al listar proyectos de Auditoría SEO: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ha ocurrido un error al listar los proyectos: {str(e)}")
import json
import duckdb
import os

os.environ['HOME'] = '/tmp'
os.environ['DUCKDB_HOME'] = '/tmp/duckdb_home'

# Crear los directorios necesarios
os.makedirs('/tmp/duckdb_home', exist_ok=True)

def ventas_por_periodo(conn):
    try:
        query = """
        SELECT strftime(Order_Purchase_Timestamp, '%Y-%m') AS periodo,
               COUNT(DISTINCT o.Order_ID) AS total_ordenes,
               SUM(oi.Price + oi.Freight_Value) AS total_ventas
        FROM orders o
        JOIN orders_items oi ON o.Order_ID = oi.Order_ID
        GROUP BY periodo
        ORDER BY periodo;
        """
        result = conn.execute(query).fetchdf().to_dict(orient="records")
        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    
def top_productos(conn):
    try:
        query = """
        SELECT p.Product_Category_Name,
               COUNT(oi.Product_ID) AS cantidad_vendida,
               SUM(oi.Price) AS ventas_totales
        FROM orders_items oi
        JOIN products p ON oi.Product_ID = p.Product_ID
        GROUP BY p.Product_Category_Name
        ORDER BY cantidad_vendida DESC
        LIMIT 10;
        """
        result = conn.execute(query).fetchdf().to_dict(orient="records")
        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def metodos_pago(conn):
    try:
        query = """
        SELECT Payment_Type,
               COUNT(*) AS cantidad,
               SUM(Payment_Value) AS total_pagado
        FROM orders_payments
        GROUP BY Payment_Type
        ORDER BY cantidad DESC;
        """
        result = conn.execute(query).fetchdf().to_dict(orient="records")
        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def lambda_handler(event, context):
    # DEBUG: Imprimir toda la estructura del evento
    print("=== EVENTO COMPLETO ===")
    print(json.dumps(event, indent=2, default=str))
    print("=======================")
    
    # DEBUG: Imprimir todas las claves del evento
    print("=== CLAVES DEL EVENTO ===")
    print(f"Keys: {list(event.keys())}")
    print("=======================")
    
    # DEBUG: Imprimir tipo del evento
    print(f"Tipo de event: {type(event)}")
    
    # Asegurar que las variables de entorno estén configuradas
    if 'HOME' not in os.environ:
        os.environ['HOME'] = '/tmp'
    if 'DUCKDB_HOME' not in os.environ:
        os.environ['DUCKDB_HOME'] = '/tmp/duckdb_home'
    
    os.makedirs('/tmp/duckdb_home', exist_ok=True)

    try:
        md_token = os.environ.get('MD_TOKEN')
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    
    conn = duckdb.connect(f'md:wadua-db?token={md_token}')
    conn.execute("USE 'wadua-db'")

    # 🔥 DEBUG: Verificar valores específicos
    http_method = event.get('httpMethod')
    path = event.get('path')
    resource = event.get('resource')
    
    print(f"httpMethod: {http_method}")
    print(f"path: {path}")
    print(f"resource: {resource}")
    
    # TEMPORAL: Intentar encontrar la ruta de otras maneras
    # Buscar en diferentes lugares comunes de API Gateway
    request_context = event.get('requestContext', {})
    api_gateway_path = request_context.get('path')
    api_gateway_resource = request_context.get('resourcePath')
    stage = request_context.get('stage')
    
    print(f"requestContext.path: {api_gateway_path}")
    print(f"requestContext.resourcePath: {api_gateway_resource}")
    print(f"requestContext.stage: {stage}")
    
    # Intentar extraer la ruta del endpoint
    endpoint = None
    if api_gateway_path:
        # Ejemplo: "/production/metodos_pago" -> "metodos_pago"
        parts = api_gateway_path.split('/')
        if len(parts) > 2:
            endpoint = parts[2]
    elif api_gateway_resource:
        endpoint = api_gateway_resource.strip('/')
    
    print(f"Endpoint detectado: {endpoint}")
    
    # TEMPORAL: Mientras debuggeamos, probar con lógica simple
    if endpoint == 'metodos_pago' or (path and 'metodos_pago' in path) or (resource and 'metodos_pago' in resource):
        return metodos_pago(conn)
    elif endpoint == 'top_productos' or (path and 'top_productos' in path) or (resource and 'top_productos' in resource):
        return top_productos(conn)
    elif endpoint == 'ventas_por_periodo' or (path and 'ventas_por_periodo' in path) or (resource and 'ventas_por_periodo' in resource):
        return ventas_por_periodo(conn)
    else:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "error": "endpoint no encontrado",
                "event_keys": list(event.keys()),
                "path": path,
                "resource": resource,
                "request_context_keys": list(request_context.keys()) if request_context else []
            })
        }


import json
import os
import mysql.connector
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI()

# Allow CORS for Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resolve absolute path to avoid CWD issues
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FILE = os.path.join(BASE_DIR, "bi_schema.json")
REPORTS_FILE = os.path.join(BASE_DIR, "bi_reports.json")

print(f"Backend initialized. Schema file: {SCHEMA_FILE}")
print(f"Reports file: {REPORTS_FILE}")

# MySQL Configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data', # Will ensure this exists
    'auth_plugin': 'mysql_native_password' # Often needed for 8.0 compatibility
}

# Ensure Database Exists
def init_mysql_db():
    # Connect to MySQL server to create DB if needed
    try:
        conn = mysql.connector.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            auth_plugin=DB_CONFIG['auth_plugin']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        conn.close()
    except Exception as e:
        print(f"Critical Error: Failed to connect/create MySQL Database: {e}")

try:
    init_mysql_db()
except:
    pass # Will fail later if critical

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

class Column(BaseModel):
    name: str
    type: str 
    primary_key: Optional[bool] = False
    foreign_key: Optional[str] = None
    description: Optional[str] = None

class Table(BaseModel):
    name: str
    columns: List[Column]
    description: Optional[str] = None

class Schema(BaseModel):
    dimensions: List[Table]
    facts: List[Table]

class JoinConfig(BaseModel):
    table: str
    join_type: str = "LEFT" 
    on_expression: str 

class ReportConfig(BaseModel):
    id: str
    category: str
    title: str
    description: Optional[str] = ""
    chart_type: str = "line"
    source_table: str
    joins: List[JoinConfig] = []
    group_by: str
    measure_formula: str
    measures: List[Dict] = []
    x_axis: Dict = {}
    filters: List[Dict] = [] 
    slices: List[str] = [] 
    image: Optional[str] = None
    base_where: Optional[str] = None

class ReportsPayload(BaseModel):
    reports: List[ReportConfig]

class EtlMapping(BaseModel):
    target_column: str
    source_expression: str # Column name or SQL expression

class EtlRequest(BaseModel):
    source_table: str
    target_table: str
    mappings: List[EtlMapping]
    truncate_target: bool = False

@app.get("/api/osaio/tables")
def get_osaio_tables():
    # Connect to osaio DB
    try:
        conn = mysql.connector.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='osaio'  # Explicitly connect to osaio
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch osaio tables: {str(e)}")

@app.get("/api/osaio/columns/{table_name}")
def get_osaio_columns(table_name: str):
    # Security check
    allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not set(table_name).issubset(allowed_chars):
         raise HTTPException(status_code=400, detail="Invalid table name")
         
    try:
        conn = mysql.connector.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='osaio'
        )
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = [row[0] for row in cursor.fetchall()]
        conn.close()
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch columns: {str(e)}")

@app.post("/api/etl/execute")
def execute_etl(request: EtlRequest):
    conn = get_db_connection() # Connects to bi_data
    cursor = conn.cursor()
    
    try:
        # 1. Truncate if requested
        if request.truncate_target:
            cursor.execute(f"TRUNCATE TABLE `{request.target_table}`")
            
        # 2. Build Insert Select Query
        # INSERT INTO bi_data.Target (c1, c2) SELECT expr1, expr2 FROM osaio.Source
        
        target_cols = [f"`{m.target_column}`" for m in request.mappings]
        source_exprs = [m.source_expression for m in request.mappings] # expressions are raw SQL
        
        sql = f"""
            INSERT INTO `{request.target_table}` ({', '.join(target_cols)})
            SELECT {', '.join(source_exprs)}
            FROM osaio.`{request.source_table}`
        """
        
        print(f"Executing ETL: {sql}")
        cursor.execute(sql)
        conn.commit()
        return {"status": "success", "message": f"Data imported from {request.source_table} to {request.target_table}"}
        
    except Exception as e:
        conn.rollback()
        print(f"ETL Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.post("/api/etl/preview")
def preview_etl(request: EtlRequest):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # 1. Fetch Raw Sample (Limit 1)
        cursor.execute(f"SELECT * FROM osaio.`{request.source_table}` LIMIT 1")
        raw_row = cursor.fetchone()
        
        if not raw_row:
             return {"raw": None, "transformed": None, "message": "Source table is empty"}

        # 2. Fetch Transformed Sample
        # Filter valid mappings
        valid_mappings = [m for m in request.mappings if m.source_expression and m.source_expression.strip()]
        
        transformed_row = {}
        if valid_mappings:
            # Construct SELECT expr AS target_col
            select_exprs = [f"{m.source_expression} AS `{m.target_column}`" for m in valid_mappings]
            sql = f"SELECT {', '.join(select_exprs)} FROM osaio.`{request.source_table}` LIMIT 1"
            
            try:
                cursor.execute(sql)
                transformed_row = cursor.fetchone()
                # Handle non-serializable types (like datetime)
                for k, v in transformed_row.items():
                    if hasattr(v, 'isoformat'):
                        transformed_row[k] = v.isoformat()
            except Exception as sql_err:
                print(f"Preview SQL Error: {sql_err}")
                # We return partial error context if possible, or just fail
                raise HTTPException(status_code=400, detail=f"SQL Expression Error: {str(sql_err)}")

        # Handle raw row serialization too
        if raw_row:
             for k, v in raw_row.items():
                 if hasattr(v, 'isoformat'):
                     raw_row[k] = v.isoformat()

        return {"raw": raw_row, "transformed": transformed_row}
        
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        print(f"Preview Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

class QueryRequest(BaseModel):
    report_id: str
    filters: Dict[str, Any] = {}
    granularity: str = "day"


def init_meta_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # MySQL syntax for Text Primary Key length requirement
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_metadata (
            `key` VARCHAR(255) PRIMARY KEY,
            value LONGTEXT
        );
    """)
    conn.commit()
    conn.close()

# Initialize on module load
try:
    init_meta_db()
except Exception as e:
    print(f"Warning: Failed to init meta db: {e}")

def get_metadata(key: str) -> Optional[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM system_metadata WHERE `key` = %s", (key,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None

def set_metadata(key: str, data: Any):
    conn = get_db_connection()
    cursor = conn.cursor()
    # MySQL Replace Into or Insert On Duplicate
    cursor.execute("REPLACE INTO system_metadata (`key`, value) VALUES (%s, %s)", (key, json.dumps(data)))
    conn.commit()
    conn.close()

@app.get("/")
def read_root():
    return {"message": "Smart Home BI Backend API (MySQL)"}

def inspect_db_schema() -> Dict:
    conn = get_db_connection()
    cursor = conn.cursor()
    
    schema_data = {"dimensions": [], "facts": []}
    
    try:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table_name in tables:
            if table_name == "system_metadata":
                continue
                
            # Classify
            category = "facts" # Default
            if table_name.startswith("Dim_"):
                category = "dimensions"
            elif table_name.startswith("Fact_"):
                category = "facts"
            else:
                continue # Ignore non-conforming tables for now
            
            # Describe
            cursor.execute(f"DESCRIBE {table_name}")
            columns = []
            for col in cursor.fetchall():
                # col: Field, Type, Null, Key, Default, Extra
                c_name = col[0]
                c_type_raw = col[1].lower()
                c_key = col[3]
                
                # Simplified Mapping
                c_type = "TEXT"
                if "int" in c_type_raw: c_type = "INTEGER"
                elif "decimal" in c_type_raw: c_type = "DECIMAL"
                elif "datetime" in c_type_raw: c_type = "DATETIME"
                elif "char" in c_type_raw: c_type = "TEXT"
                
                columns.append({
                    "name": c_name,
                    "type": c_type,
                    "primary_key": c_key == 'PRI',
                    "description": "" 
                })
            
            table_obj = {
                "name": table_name,
                "columns": columns,
                "description": f"Table {table_name}"
            }
            
            schema_data[category].append(table_obj)
            
    except Exception as e:
        print(f"Error inspecting DB schema: {e}")
        return {"dimensions": [], "facts": [], "debug_error": str(e)}
    finally:
        conn.close()
        
    return schema_data

@app.get("/api/schema", response_model=Dict) 
def get_schema():
    # Direct DB Inspection
    return inspect_db_schema()

@app.post("/api/schema")
def update_schema(schema: Schema):
    # Since we are using Direct DB Inspection, we do not support updating schema via JSON file anymore.
    # Users should modify the DB schema directly (ALTER TABLE).
    return {"status": "info", "message": "Schema is read directly from MySQL. Please use database tools to modify schema."}

@app.get("/api/export")
def export_schema():
    if not os.path.exists(SCHEMA_FILE):
         return {"dimensions": [], "facts": []}
    with open(SCHEMA_FILE, "r") as f:
        data = json.load(f)
    return JSONResponse(
        content=data,
        headers={"Content-Disposition": "attachment; filename=bi_schema.json"}
    )

def generate_create_table_sql(table: Table) -> str:
    cols = []
    primary_keys = []
    
    for col in table.columns:
        # Type Mapping SQLite/Generic -> MySQL
        mysql_type = "VARCHAR(255)"
        if col.type == "INTEGER":
            mysql_type = "INT"
        elif col.type == "REAL":
            mysql_type = "DOUBLE"
        elif col.type == "TEXT":
             mysql_type = "VARCHAR(255)"
        elif col.type == "BOOLEAN":
             mysql_type = "TINYINT(1)"
        
        c_def = f"`{col.name}` {mysql_type}"
        cols.append(c_def)
        
        if col.primary_key:
            primary_keys.append(f"`{col.name}`")
            
    sql = f"CREATE TABLE IF NOT EXISTS `{table.name}` ({', '.join(cols)}"
    if primary_keys:
        sql += f", PRIMARY KEY ({', '.join(primary_keys)})"
    sql += ");"
    
    return sql

def get_mysql_type(col_type: str) -> str:
    if col_type == "INTEGER": return "INT"
    if col_type == "REAL": return "DOUBLE"
    if col_type == "BOOLEAN": return "TINYINT(1)"
    return "VARCHAR(255)"

def sync_table_columns(cursor, table: Table):
    # Get existing columns
    cursor.execute(f"DESCRIBE `{table.name}`")
    existing_cols = {row[0]: row[1] for row in cursor.fetchall()} # name -> type(str)
    
    for col in table.columns:
        target_type = get_mysql_type(col.type)
        
        if col.name not in existing_cols:
            # ADD COLUMN
            print(f"Adding column {col.name} to {table.name}")
            sql = f"ALTER TABLE `{table.name}` ADD COLUMN `{col.name}` {target_type}"
            cursor.execute(sql)
        else:
            # Check type mismatch (Basic check, avoiding complex parsing of 'int(11)' vs 'INT')
            # For now, we trust the DB unless it's a major mismatch or user requests force update
            # We implemented ADD only for safety in this iteration as requested
            pass

@app.post("/api/apply-schema")
def apply_schema():
    if not os.path.exists(SCHEMA_FILE):
        raise HTTPException(status_code=404, detail="Schema file not found")
        
    with open(SCHEMA_FILE, "r") as f:
        data = json.load(f)
    
    schema = Schema(**data)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Create/Sync Dimensions
        for table in schema.dimensions:
            # 1. Create if not exists
            sql = generate_create_table_sql(table)
            cursor.execute(sql)
            # 2. Sync Columns
            sync_table_columns(cursor, table)
            
        # Create/Sync Facts
        for table in schema.facts:
            # 1. Create if not exists
            sql = generate_create_table_sql(table)
            cursor.execute(sql)
            # 2. Sync Columns
            sync_table_columns(cursor, table)
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Schema Apply Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"status": "success", "message": "Schema synced to MySQL database (Created missing tables & Added missing columns)"}

@app.get("/api/reports")
def get_reports():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT config FROM bi_reports")
        reports = [json.loads(row[0]) for row in cursor.fetchall()]
        return {"reports": reports}
    except Exception as e:
        print(f"Error fetching reports: {e}")
        return {"reports": [], "error": str(e)}
    finally:
        conn.close()

@app.post("/api/reports")
def save_report(report: ReportConfig):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        r_data = report.dict()
        config_json = json.dumps(r_data)
        
        # Upsert
        sql = """
        INSERT INTO bi_reports (id, category, title, description, config)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category = VALUES(category),
            title = VALUES(title),
            description = VALUES(description),
            config = VALUES(config)
        """
        cursor.execute(sql, (report.id, report.category, report.title, report.description, config_json))
        conn.commit()
        return {"status": "success", "message": "Report saved"}
    except Exception as e:
        print(f"Error saving report: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM bi_reports WHERE id = %s", (report_id,))
        conn.commit()
        return {"status": "success", "message": "Report deleted"}
    except Exception as e:
        print(f"Error deleting report: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str):
    data = get_reports() 
    if isinstance(data, dict):
         reports = data.get("reports", [])
    else:
         reports = []
         
    reports = [r for r in reports if r["id"] != report_id]
    
    new_data = {"reports": reports}
    
    # Save to File
    with open(REPORTS_FILE, "w") as f:
        json.dump(new_data, f, indent=2)
    # Save to DB
    set_metadata("bi_reports", new_data)
        
    return {"status": "success"}

@app.post("/api/query")
def execute_query(query: QueryRequest):
    reports_data = get_reports()
    report_dict = next((r for r in reports_data.get("reports", []) if r["id"] == query.report_id), None)
    
    if not report_dict:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report = ReportConfig(**report_dict)

    source_table = report.source_table
    measure_formula = report.measure_formula
    
    config_group_by = report.group_by
    
    # Granularity Logic
    # If the report config already defines a complex group_by (like DATE_FORMAT), use it.
    # Otherwise, apply default string slicing if granularity is requested.
    group_expression = config_group_by
    
    if query.granularity and "(" not in config_group_by:
         if "time" in config_group_by.lower() or "date" in config_group_by.lower():
            if query.granularity == "year":
                group_expression = f"DATE_FORMAT({config_group_by}, '%Y')"
            elif query.granularity == "month":
                group_expression = f"DATE_FORMAT({config_group_by}, '%Y-%m')"
            elif query.granularity == "day":
                group_expression = f"DATE_FORMAT({config_group_by}, '%Y-%m-%d')"
    
    join_clause = ""
    for j in report.joins:
        join_clause += f" {j.join_type} JOIN `{j.table}` ON {j.on_expression}"
        
    sql = f"""
        SELECT 
            {group_expression} as x_result, 
            {measure_formula} 
        FROM `{source_table}`
        {join_clause}
    """
    
    params = []
    where_clauses = []
    
    for col, val in query.filters.items():
        if val:
            # Handle list for IN clause
            # Prefix with source_table to avoid ambiguity in JOINs
            prefixed_col = f"`{source_table}`.`{col}`"
            if isinstance(val, list):
                if not val: continue
                placeholders = ', '.join(['%s'] * len(val))
                where_clauses.append(f"{prefixed_col} IN ({placeholders})")
                params.extend(val)
            else:
                where_clauses.append(f"{prefixed_col} = %s")
                params.append(val)
            
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
        
    if report.base_where:
        if "WHERE" in sql:
            sql += f" AND ({report.base_where})"
        else:
            sql += f" WHERE ({report.base_where})"
        
    sql += f" GROUP BY x_result ORDER BY x_result"
    
    print(f"Executing SQL: {sql} | Params: {params}") 

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, tuple(params))
        
        # Get column names to identify series (anything besides x_result)
        columns = [col[0] for col in cursor.description]
        y_column_indices = [i for i, name in enumerate(columns) if name != 'x_result']
        series_names = [columns[i] for i in y_column_indices]
        
        rows = cursor.fetchall()
        
        # Format X-Axis and Series
        x_data = []
        series_data = {name: [] for name in series_names}
        
        for row in rows:
            x_data.append(row[0]) # x_result is first
            for i, idx in enumerate(y_column_indices):
                val = row[idx]
                series_data[series_names[i]].append(float(val) if val is not None else 0)
            
        return {
            "x_axis": x_data,
            "series": [
                {
                    "name": name,
                    "data": series_data[name]
                } for name in series_names
            ]
        }
    except Exception as e:
        print(f"Query Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.get("/api/data/{table_name}")
def get_table_data(table_name: str):
    allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not set(table_name).issubset(allowed_chars):
         raise HTTPException(status_code=400, detail="Invalid table name")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True) # Return dicts
    try:
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
             raise HTTPException(status_code=404, detail="Table not found")

        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 100")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.get("/api/filter-values/{table_name}/{column_name}")
def get_filter_values(table_name: str, column_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Basic validation on identifiers
        sql = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL AND `{column_name}` != '' ORDER BY `{column_name}`"
        cursor.execute(sql)
        values = [row[0] for row in cursor.fetchall()]
        return {"values": values}
    except Exception as e:
        print(f"Error fetching filter values: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

import json
import os
import sqlite3
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
DB_FILE = os.path.join(BASE_DIR, "bi_data.db")
REPORTS_FILE = os.path.join(BASE_DIR, "bi_reports.json")

print(f"Backend initialized. Schema file: {SCHEMA_FILE}")
print(f"Reports file: {REPORTS_FILE}")

class Column(BaseModel):
    name: str
    type: str # INTEGER, TEXT, REAL, etc.
    primary_key: Optional[bool] = False
    foreign_key: Optional[str] = None # e.g. "Dim_User.user_id"
    description: Optional[str] = None # Chinese remark

class Table(BaseModel):
    name: str
    columns: List[Column]
    description: Optional[str] = None # Chinese remark

class Schema(BaseModel):
    dimensions: List[Table]
    facts: List[Table]

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# ... (Imports remain, remove init_meta_tables, migrate_json_to_db)

@app.get("/")
def read_root():
    return {"message": "Smart Home BI Backend API"}

@app.get("/api/schema", response_model=Dict) 
def get_schema():
    if not os.path.exists(SCHEMA_FILE):
        return {"dimensions": [], "facts": [], "debug_error": f"File not found at {SCHEMA_FILE}", "cwd": os.getcwd()}
    
    with open(SCHEMA_FILE, "r") as f:
        try:
            content = f.read()
            if not content.strip():
                 return {"dimensions": [], "facts": [], "debug_error": "File empty"}
            data = json.loads(content)
            return data
        except json.JSONDecodeError as e:
            return {"dimensions": [], "facts": [], "debug_error": str(e)}

@app.post("/api/schema")
def update_schema(schema: Schema):
    with open(SCHEMA_FILE, "w") as f:
        json.dump(schema.dict(), f, indent=2)
    return {"status": "success", "message": "Schema updated successfully"}

@app.get("/api/export")
def export_schema():
    # Just serve the file directly
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
    for col in table.columns:
        c_def = f"{col.name} {col.type}"
        if col.primary_key:
            c_def += " PRIMARY KEY"
        cols.append(c_def)
    
    return f"CREATE TABLE IF NOT EXISTS {table.name} ({', '.join(cols)});"

@app.post("/api/apply-schema")
def apply_schema():
    # 1. Load Schema from JSON
    if not os.path.exists(SCHEMA_FILE):
        raise HTTPException(status_code=404, detail="Schema file not found")
        
    with open(SCHEMA_FILE, "r") as f:
        data = json.load(f)
    
    schema = Schema(**data)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Create Dimensions
        for table in schema.dimensions:
            sql = generate_create_table_sql(table)
            cursor.execute(sql)
            
        # Create Facts
        for table in schema.facts:
            sql = generate_create_table_sql(table)
            cursor.execute(sql)
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"status": "success", "message": "Schema applied to SQLite database"}


class JoinConfig(BaseModel):
    table: str
    join_type: str = "LEFT" # LEFT, INNER
    on_expression: str # e.g. "Fact_Order.user_surrogate_key = Dim_User.surrogate_key"

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
    filters: List[Dict] = [] # Pre-defined filters
    slices: List[str] = [] # Columns available for user filtering
    image: Optional[str] = None

class ReportsPayload(BaseModel):
    reports: List[ReportConfig]

class QueryRequest(BaseModel):
    report_id: str
    filters: Dict[str, Any] = {}
    granularity: str = "day"

REPORTS_FILE = os.path.join(BASE_DIR, "bi_reports.json")

@app.get("/api/reports")
def get_reports():
    if not os.path.exists(REPORTS_FILE):
        return {"reports": []}
    with open(REPORTS_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {"reports": []}

@app.post("/api/reports")
def save_reports(payload: ReportsPayload):
    with open(REPORTS_FILE, "w") as f:
        json.dump(payload.dict(), f, indent=2)
    return {"status": "success"}

@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str):
    data = get_reports() # Re-use get_reports safely
    if isinstance(data, dict):
         reports = data.get("reports", [])
    else:
         reports = []
         
    reports = [r for r in reports if r["id"] != report_id]
    
    with open(REPORTS_FILE, "w") as f:
        json.dump({"reports": reports}, f, indent=2)
    return {"status": "success"}

@app.post("/api/query")
def execute_query(query: QueryRequest):
    # Load report definition
    reports_data = get_reports()
    report_dict = next((r for r in reports_data.get("reports", []) if r["id"] == query.report_id), None)
    
    if not report_dict:
        raise HTTPException(status_code=404, detail="Report not found")
    
    # Convert dict back to model for easier handling (optional, but good for type safety)
    # handling optional fields carefully if loading from loose JSON
    report = ReportConfig(**report_dict)

    source_table = report.source_table
    measure_formula = report.measure_formula
    
    # 1. Build Base SQL
    # "SELECT {group_by} as x_result, {measure} as y_result FROM {table} {joins}"
    
    # Granularity Logic
    # Check if group_by column looks like a time column (very basic heuristic or config based)
    # For MVP, we stick to the provided group_by, but if it is 'time_key' we apply granularity
    group_col = report.group_by
    group_expression = group_col
    
    # If using 'time_key' and granularity is requested (and it's a date string YYYYMMDD or similar)
    if "time" in group_col.lower() or "date" in group_col.lower():
        if query.granularity == "year":
            group_expression = f"substr({group_col}, 1, 4)"
        elif query.granularity == "month":
            group_expression = f"substr({group_col}, 1, 6)"
        # else day -> no change
    
    # 2. Build Joins
    join_clause = ""
    for j in report.joins:
        join_clause += f" {j.join_type} JOIN {j.table} ON {j.on_expression}"
        
    sql = f"""
        SELECT 
            {group_expression} as x_result, 
            {measure_formula} as y_result 
        FROM {source_table}
        {join_clause}
    """
    
    # 3. Build Where Clause
    params = []
    where_clauses = []
    
    # User selected filters (slices)
    for col, val in query.filters.items():
        if val:
            # Basic validation: ensure col is in slices or pre-defined filters
            # For flexibility, we allow it if it looks safe (alphanumeric_dot) for now
            where_clauses.append(f"{col} = ?")
            params.append(val)
            
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
        
    sql += f" GROUP BY {group_expression} ORDER BY {group_expression}"
    
    print(f"Executing SQL: {sql} | Params: {params}") # Debug logging

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return {
            "x_axis": [row[0] for row in rows],
            "series": [
                {
                    "name": "Value",
                    "data": [row[1] for row in rows]
                }
            ]
        }
    except Exception as e:
        print(f"Query Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.get("/api/data/{table_name}")
def get_table_data(table_name: str):
    # Security check (simple)
    allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not set(table_name).issubset(allowed_chars):
         raise HTTPException(status_code=400, detail="Invalid table name")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
             raise HTTPException(status_code=404, detail="Table not found")

        cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
        rows = cursor.fetchall()
        return {"data": [dict(row) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

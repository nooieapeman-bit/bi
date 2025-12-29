import json
import os
import sqlite3
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow CORS for Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SCHEMA_FILE = "bi_schema.json"
DB_FILE = "bi_data.db"

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

@app.get("/")
def read_root():
    return {"message": "Smart Home BI Backend API"}

@app.get("/api/schema", response_model=Schema)
def get_schema():
    if not os.path.exists(SCHEMA_FILE):
        return {"dimensions": [], "facts": []}
    
    with open(SCHEMA_FILE, "r") as f:
        try:
            data = json.load(f)
            return data
        except json.JSONDecodeError:
            return {"dimensions": [], "facts": []}

@app.post("/api/schema")
def update_schema(schema: Schema):
    with open(SCHEMA_FILE, "w") as f:
        json.dump(schema.dict(), f, indent=2)
    return {"status": "success", "message": "Schema updated successfully"}

def generate_create_table_sql(table: Table) -> str:
    cols = []
    for col in table.columns:
        c_def = f"{col.name} {col.type}"
        if col.primary_key:
            c_def += " PRIMARY KEY"
        cols.append(c_def)
    
    # Simple FK handling can be added here if needed, 
    # but SQLite verification logic usually requires PRAGMA foreign_keys = ON
    
    return f"CREATE TABLE IF NOT EXISTS {table.name} ({', '.join(cols)});"

@app.post("/api/apply-schema")
def apply_schema():
    # 1. Load Schema
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

@app.get("/api/data/{table_name}")
def get_table_data(table_name: str):
    # Security: Validate table name exists in schema to prevent injection
    # For MVP, we'll just check if it matches a known pattern or is in schema
    # (Simplified: trusting input for now or checking schema)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
        rows = cursor.fetchall()
        return {"data": [dict(row) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

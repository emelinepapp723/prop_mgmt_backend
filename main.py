from fastapi import FastAPI, Depends, HTTPException
from google.cloud import bigquery
from pydantic import BaseModel
from datetime import date

app = FastAPI()

# -----------------------------
# Project / Dataset
# -----------------------------
PROJECT_ID = "sp26-mgmt"
DATASET = "property_mgmt"

# -----------------------------
# BigQuery client dependency
# -----------------------------
def get_bq_client():
    return bigquery.Client()

# -----------------------------
# Properties Models
# -----------------------------
class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: str | None = None
    monthly_rent: float | None = None

# For PUT (full replace → same schema as create)
class PropertyUpdate(PropertyCreate):
    pass

# -----------------------------
# Properties Endpoints
# -----------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    return [dict(row) for row in bq.query(query).result()]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    rows = [dict(row) for row in bq.query(query, job_config=job_config).result()]
    if not rows:
        raise HTTPException(status_code=404, detail="Property not found")

    return rows[0]

@app.post("/properties", status_code=201)
def create_property(property: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):

    # Generate next property_id
    id_query = f"""
        SELECT IFNULL(MAX(property_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
    """
    next_id = [row["next_id"] for row in bq.query(id_query).result()][0]

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
        VALUES (
            @property_id, @name, @address, @city, @state,
            @postal_code, @property_type, @tenant_name, @monthly_rent
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("name", "STRING", property.name),
            bigquery.ScalarQueryParameter("address", "STRING", property.address),
            bigquery.ScalarQueryParameter("city", "STRING", property.city),
            bigquery.ScalarQueryParameter("state", "STRING", property.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", property.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property.monthly_rent),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Property created", "property_id": next_id}

# -----------------------------
# PUT (FULL REPLACEMENT)
# -----------------------------
@app.put("/properties/{property_id}")
def update_property(
    property_id: int,
    property: PropertyUpdate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    # Check if property exists
    check_query = f"""
        SELECT COUNT(1) as cnt
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    if list(bq.query(check_query, job_config=check_config).result())[0]["cnt"] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    # Full overwrite of all fields
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET
            name = @name,
            address = @address,
            city = @city,
            state = @state,
            postal_code = @postal_code,
            property_type = @property_type,
            tenant_name = @tenant_name,
            monthly_rent = @monthly_rent
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("name", "STRING", property.name),
            bigquery.ScalarQueryParameter("address", "STRING", property.address),
            bigquery.ScalarQueryParameter("city", "STRING", property.city),
            bigquery.ScalarQueryParameter("state", "STRING", property.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", property.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property.monthly_rent),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Property updated", "property_id": property_id}

# -----------------------------
# DELETE PROPERTY
# -----------------------------
@app.delete("/properties/{property_id}")
def delete_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    # Check if property exists
    check_query = f"""
        SELECT COUNT(1) as cnt
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    if list(bq.query(check_query, job_config=check_config).result())[0]["cnt"] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    # Delete property
    delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    delete_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    bq.query(delete_query, job_config=delete_config).result()

    return {"message": "Property deleted", "property_id": property_id}

# -----------------------------
# Income Models
# -----------------------------
class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: str | None = None

# -----------------------------
# Income Endpoints
# -----------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    return [dict(row) for row in bq.query(query, job_config=job_config).result()]

@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):

    check_query = f"""
        SELECT COUNT(1) as cnt
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id=@property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    if list(bq.query(check_query, job_config=job_config).result())[0]["cnt"] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    id_query = f"""
        SELECT IFNULL(MAX(income_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.income`
    """
    next_id = [row["next_id"] for row in bq.query(id_query).result()][0]

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
        VALUES (@income_id, @property_id, @amount, @date, @description)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("income_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
            bigquery.ScalarQueryParameter("date", "DATE", income.date),
            bigquery.ScalarQueryParameter("description", "STRING", income.description),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Income created", "income_id": next_id}

# -----------------------------
# Expenses Models
# -----------------------------
class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: str | None = None
    description: str | None = None

# -----------------------------
# Expenses Endpoints
# -----------------------------
@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    return [dict(row) for row in bq.query(query, job_config=job_config).result()]

@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):

    check_query = f"""
        SELECT COUNT(1) as cnt
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id=@property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    if list(bq.query(check_query, job_config=job_config).result())[0]["cnt"] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    id_query = f"""
        SELECT IFNULL(MAX(expense_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.expenses`
    """
    next_id = [row["next_id"] for row in bq.query(id_query).result()][0]

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
        VALUES (
            @expense_id, @property_id, @amount, @date,
            @category, @vendor, @description
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expense_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.amount),
            bigquery.ScalarQueryParameter("date", "DATE", expense.date),
            bigquery.ScalarQueryParameter("category", "STRING", expense.category),
            bigquery.ScalarQueryParameter("vendor", "STRING", expense.vendor),
            bigquery.ScalarQueryParameter("description", "STRING", expense.description),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Expense created", "expense_id": next_id}
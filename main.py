from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from datetime import date

app = FastAPI()

PROJECT_ID = "sp26-mgmt"
DATASET = "property_mgmt"


# BigQuery client dependency
def get_bq_client():
    return bigquery.Client()

# -----------------------------
# Properties Endpoints
# -----------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    rows = [dict(row) for row in results]
    if not rows:
        raise HTTPException(status_code=404, detail="Property not found")
    return rows[0]

# -----------------------------
# Income Endpoints
# -----------------------------
class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: str | None = None

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    results = bq.query(query, job_config=job_config).result()
    return [dict(row) for row in results]

@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    # Check if property exists
    prop_check_query = f"SELECT COUNT(1) as cnt FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id=@property_id"
    prop_check_job = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    if [row['cnt'] for row in bq.query(prop_check_query, job_config=prop_check_job).result()][0] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    # Generate next income_id
    id_query = f"SELECT IFNULL(MAX(income_id), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET}.income`"
    next_id = [row['next_id'] for row in bq.query(id_query).result()][0]

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, description)
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
    return {"message": "Income record created successfully", "income_id": next_id}

# -----------------------------
# Expenses Endpoints
# -----------------------------
class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: str | None = None
    description: str | None = None

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    results = bq.query(query, job_config=job_config).result()
    return [dict(row) for row in results]

@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    # Check if property exists
    prop_check_query = f"SELECT COUNT(1) as cnt FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id=@property_id"
    prop_check_job = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )
    if [row['cnt'] for row in bq.query(prop_check_query, job_config=prop_check_job).result()][0] == 0:
        raise HTTPException(status_code=404, detail="Property not found")

    # Generate next expense_id
    id_query = f"SELECT IFNULL(MAX(expense_id), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET}.expenses`"
    next_id = [row['next_id'] for row in bq.query(id_query).result()][0]

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
        (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
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
    return {"message": "Expense record created successfully", "expense_id": next_id}

# -----------------------------

import os
import trino
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", 8089)),
        user=os.getenv("TRINO_USER", "bot_user"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "gold"),
    )

def search_jobs(keyword: str, limit: int = 100):
    """Tìm kiếm việc làm dựa trên keyword (áp dụng cho title)."""
    query = """
        SELECT f.job_title, c.company_name, f.salary_min, f.salary_max, f.salary_currency, f.job_url, l.location_name, f.deadline_date, f.inserted_at, lv.level_name
        FROM fact_job_postings f
        LEFT JOIN dim_company c ON f.company_id = c.company_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LEFT JOIN dim_level lv ON f.level_id = lv.level_id
        WHERE LOWER(f.job_title) LIKE ?
        ORDER BY f.ingested_at DESC
        LIMIT ?
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params=(f"%{keyword.lower()}%", limit))
            columns = [desc[0] for desc in cur.description]
            results = [dict(zip(columns, row)) for row in cur.fetchall()]
            return results
    except Exception as e:
        print(f"Error executing Trino query: {e}")
        return []

def get_stats():
    """Lấy số lượng job theo từng nguồn."""
    query = """
        SELECT source, COUNT(*) as count 
        FROM fact_job_postings 
        GROUP BY source
        ORDER BY count DESC
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        print(f"Error fetching stats: {e}")
        return []

def get_top_skills(limit: int = 10):
    """Lấy top kỹ năng được yêu cầu nhiều nhất."""
    query = """
        SELECT s.skill_name, COUNT(b.job_id) as demand
        FROM bridge_job_skills b
        JOIN dim_skill s ON b.skill_id = s.skill_id
        GROUP BY s.skill_name
        ORDER BY demand DESC
        LIMIT ?
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params=(limit,))
            return cur.fetchall()
    except Exception as e:
        print(f"Error fetching top skills: {e}")
        return []

def get_top_locations(limit: int = 5):
    """Lấy top địa điểm có nhiều việc làm."""
    query = """
        SELECT l.location_name, COUNT(f.job_id) as job_count
        FROM fact_job_postings f
        JOIN dim_location l ON f.location_id = l.location_id
        GROUP BY l.location_name
        ORDER BY job_count DESC
        LIMIT ?
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params=(limit,))
            return cur.fetchall()
    except Exception as e:
        print(f"Error fetching top locations: {e}")
        return []

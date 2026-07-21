import csv
import io
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

API_KEY = os.environ.get("DGIS_API_KEY", "")
CATALOG_URL = "https://catalog.api.2gis.com/3.0/items"

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "leads.db"

app = FastAPI(title="GIS Lead Finder")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            niche TEXT,
            city TEXT,
            name TEXT,
            address TEXT,
            phone TEXT,
            rubric TEXT,
            has_website INTEGER,
            source_id TEXT UNIQUE,
            created_at TEXT
        )
        """
    )
    try:
        conn.execute("ALTER TABLE leads ADD COLUMN status TEXT DEFAULT 'new'")
    except sqlite3.OperationalError:
        pass  # колонка уже добавлена в прошлый запуск
    conn.commit()
    conn.close()


init_db()


class SearchRequest(BaseModel):
    niche: str
    city: str
    count: int = 30


class LeadUpdate(BaseModel):
    phone: str | None = None
    status: str | None = None


def extract_contacts(item: dict) -> dict:
    """2GIS отдаёт контакты в item['contact_groups'][*]['contacts'][*].
    Структура полей проверена на момент написания — сверься с актуальной
    документацией https://docs.2gis.com/ru/api/search/places/reference,
    если формат ответа изменится."""
    phones = []
    has_website = False
    for group in item.get("contact_groups") or []:
        for c in group.get("contacts") or []:
            ctype = c.get("type")
            if ctype == "phone":
                val = c.get("value") or c.get("text")
                if val:
                    phones.append(val)
            elif ctype in ("website", "webpage"):
                has_website = True
    return {"phones": phones, "has_website": has_website}


async def fetch_2gis(niche: str, city: str, count: int) -> list[dict]:
    if not API_KEY:
        raise HTTPException(500, "DGIS_API_KEY не задан — добавь ключ в .env")

    query = f"{niche}, {city}"
    results: list[dict] = []
    page = 1
    # Этот ключ 2GIS ограничивает page_size диапазоном 1-10 (проверено на
    # реальном ответе API — при большем значении 2GIS отдаёт HTTP 200, но
    # с ошибкой внутри meta.code=400, а не HTTP-ошибкой).
    page_size = min(max(count, 1), 10)
    max_pages = 20

    async with httpx.AsyncClient(timeout=20) as client:
        while len(results) < count and page <= max_pages:
            params = {
                "q": query,
                "key": API_KEY,
                "page": page,
                "page_size": page_size,
                "fields": "items.contact_groups,items.point,items.address_name,items.rubrics",
                "locale": "ru_RU",
            }
            resp = await client.get(CATALOG_URL, params=params)
            if resp.status_code != 200:
                raise HTTPException(
                    resp.status_code, f"2GIS API: {resp.text[:300]}"
                )
            data = resp.json()
            meta = data.get("meta") or {}
            if meta.get("code") != 200:
                raise HTTPException(
                    502, f"2GIS API: {meta.get('error', {}).get('message', meta)}"
                )
            items = (data.get("result") or {}).get("items") or []
            if not items:
                break
            results.extend(items)
            page += 1

    return results[:count]


@app.post("/api/search")
async def search(req: SearchRequest):
    items = await fetch_2gis(req.niche, req.city, req.count)

    conn = get_db()
    saved = []
    for item in items:
        contacts = extract_contacts(item)
        rubrics = item.get("rubrics") or []
        rubric = rubrics[0].get("name", "") if rubrics else ""

        row = {
            "niche": req.niche,
            "city": req.city,
            "name": item.get("name", ""),
            "address": item.get("address_name", ""),
            "phone": ", ".join(contacts["phones"]),
            "rubric": rubric,
            "has_website": int(contacts["has_website"]),
            "source_id": item.get("id", ""),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        conn.execute(
            """
            INSERT INTO leads
                (niche, city, name, address, phone, rubric, has_website, source_id, created_at)
            VALUES
                (:niche, :city, :name, :address, :phone, :rubric, :has_website, :source_id, :created_at)
            ON CONFLICT(source_id) DO UPDATE SET
                phone=excluded.phone,
                has_website=excluded.has_website,
                created_at=excluded.created_at
            """,
            row,
        )
        saved.append(row)
    conn.commit()
    conn.close()

    return {"total": len(saved), "items": saved}


@app.get("/api/leads")
def list_leads():
    conn = get_db()
    rows = conn.execute("SELECT * FROM leads ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.patch("/api/leads/{lead_id}")
def update_lead(lead_id: int, patch: LeadUpdate):
    fields = {}
    if patch.phone is not None:
        fields["phone"] = patch.phone
    if patch.status is not None:
        fields["status"] = patch.status
    if not fields:
        return {"ok": True}

    conn = get_db()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(
        f"UPDATE leads SET {set_clause} WHERE id = ?",
        (*fields.values(), lead_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/api/leads/{lead_id}")
def delete_lead(lead_id: int):
    conn = get_db()
    conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/api/leads")
def clear_leads():
    conn = get_db()
    conn.execute("DELETE FROM leads")
    conn.commit()
    conn.close()
    return {"ok": True}


STATUS_LABELS = {
    "new": "Новый",
    "called": "Звонил",
    "interested": "Заинтересован",
    "not_interested": "Не интересно",
    "has_site": "Уже есть сайт",
    "unreachable": "Недозвонился",
}


@app.get("/api/export")
def export_csv():
    conn = get_db()
    rows = conn.execute("SELECT * FROM leads ORDER BY created_at DESC").fetchall()
    conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Ниша", "Город", "Название", "Адрес", "Рубрика", "Телефон", "Статус"])
    for r in rows:
        status_label = STATUS_LABELS.get(r["status"], r["status"] or "Новый")
        writer.writerow(
            [r["niche"], r["city"], r["name"], r["address"], r["rubric"], r["phone"], status_label]
        )
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


app.mount("/", StaticFiles(directory=BASE_DIR / "static", html=True), name="static")

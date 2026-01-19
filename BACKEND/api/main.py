from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="API Écologie")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CUBEJS_API = "https://api.indicateurs.ecologie.gouv.fr/cubejs-api/v1"


@app.get("/")
async def root():
    return {"message": "API Écologie", "docs": "/docs"}


async def query_cube(client, headers, cube: str, measure: str, commune: str):
    """Helper pour requêter un cube."""
    query = {
        "measures": [measure],
        "dimensions": [f"{cube}.libelle_commune", f"{cube}.annee"],
        "filters": [
            {"member": f"{cube}.libelle_commune", "operator": "contains", "values": [commune]},
            {"member": f"{cube}.annee", "operator": "gte", "values": ["2020"]}
        ],
        "limit": 10
    }
    response = await client.get(
        f"{CUBEJS_API}/load",
        headers=headers,
        params={"query": str(query).replace("'", '"')},
        timeout=30.0
    )
    if response.status_code == 200:
        return response.json().get("data", [])
    return []


@app.get("/indicateurs")
async def get_indicateurs(commune: str = Query(..., description="Nom de la commune")):
    """Récupère les indicateurs écologiques pour une commune."""
    token = os.getenv("token")
    if not token:
        raise HTTPException(status_code=401, detail="Token manquant dans .env")
    
    headers = {"Authorization": f"Bearer {token}"}
    
    async with httpx.AsyncClient() as client:
        # Consommation énergie
        energie = await query_cube(client, headers, "conso_enaf_com", "conso_enaf_com.id_611", commune)
        
        # Prélèvement eau
        eau = await query_cube(client, headers, "prelevement_eau_usage_com", "prelevement_eau_usage_com.id_638", commune)
        
        return {"energie": energie, "eau": eau}

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
    # Essayer d'abord une recherche exacte (insensible à la casse)
    commune_formatted = commune.strip().title()
    
    query = {
        "measures": [measure],
        "dimensions": [f"{cube}.libelle_commune", f"{cube}.annee"],
        "filters": [
            {"member": f"{cube}.libelle_commune", "operator": "equals", "values": [commune_formatted]},
            {"member": f"{cube}.annee", "operator": "gte", "values": ["2020"]}
        ],
        "order": [[f"{cube}.annee", "desc"]],
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
        # Mobilité - Aménagements cyclables
        mobilite = await query_cube(client, headers, "lineaire_cyclable_habitant_com", "lineaire_cyclable_habitant_com.id_839", commune)
        
        # Énergie - Puissance électrique installée
        energie = await query_cube(client, headers, "puissance_elec_installee_com", "puissance_elec_installee_com.id_636", commune)
        
        # Sobriété - Émissions GES par habitant
        ges = await query_cube(client, headers, "emission_ges_hab_com", "emission_ges_hab_com.id_2", commune)
        
        # Biodiversité - Séquestration CO2
        biodiversite = await query_cube(client, headers, "sequestr_nette_co2_com", "sequestr_nette_co2_com.id_615", commune)
        
        # Eau - Prélèvements
        eau = await query_cube(client, headers, "prelevement_eau_usage_com", "prelevement_eau_usage_com.id_638", commune)
        
        return {
            "mobilite": mobilite,
            "energie": energie,
            "ges": ges,
            "biodiversite": biodiversite,
            "eau": eau
        }

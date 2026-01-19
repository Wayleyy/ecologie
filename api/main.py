from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="API Écologie France",
    description="API pour accéder aux données écologiques françaises via data.gouv.fr",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# URLs de base
TABULAR_API_BASE = "https://tabular-api.data.gouv.fr/api"
INDICATEURS_API_BASE = "https://api.indicateurs.ecologie.gouv.fr"
CUBEJS_API = f"{INDICATEURS_API_BASE}/cubejs-api/v1"


# ============ API Tabulaire data.gouv.fr ============

@app.get("/tabular/resources/{resource_id}")
async def get_resource_info(resource_id: str):
    """Récupère les informations d'une ressource tabulaire."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{TABULAR_API_BASE}/resources/{resource_id}/")
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Ressource non trouvée")
        return response.json()


@app.get("/tabular/resources/{resource_id}/data")
async def get_resource_data(
    resource_id: str,
    page: int = Query(1, ge=1, description="Numéro de page"),
    page_size: int = Query(20, ge=1, le=100, description="Taille de page")
):
    """Récupère les données d'une ressource tabulaire avec pagination."""
    async with httpx.AsyncClient() as client:
        params = {"page": page, "page_size": page_size}
        response = await client.get(
            f"{TABULAR_API_BASE}/resources/{resource_id}/data/",
            params=params
        )
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Erreur lors de la récupération des données")
        return response.json()


@app.get("/tabular/resources/{resource_id}/profile")
async def get_resource_profile(resource_id: str):
    """Récupère le profil (métadonnées des colonnes) d'une ressource."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{TABULAR_API_BASE}/resources/{resource_id}/profile/")
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Profil non disponible")
        return response.json()


# ============ Hub Indicateurs Transition Écologique ============

@app.get("/indicateurs/cubes")
async def get_indicateurs_cubes(
    token: Optional[str] = Query(None, description="Token JWT (optionnel si défini dans .env)")
):
    """Liste tous les cubes (jeux de données) disponibles."""
    jwt_token = token or os.getenv("token")
    if not jwt_token:
        raise HTTPException(status_code=401, detail="Token JWT manquant")
    
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = await client.get(f"{CUBEJS_API}/meta", headers=headers, timeout=10.0)
        data = response.json()
        
        # Extraire la liste simplifiée des cubes
        cubes = []
        for cube in data.get("cubes", []):
            cubes.append({
                "name": cube["name"],
                "title": cube.get("title", cube["name"]),
                "measures": [m["name"] for m in cube.get("measures", [])],
                "dimensions": [d["name"] for d in cube.get("dimensions", [])]
            })
        return {"cubes": cubes, "total": len(cubes)}


@app.get("/indicateurs/query")
async def query_indicateurs(
    cube: str = Query(..., description="Nom du cube (ex: tri_biodechets_dpt)"),
    measures: str = Query(..., description="Mesures séparées par virgule (ex: tri_biodechets_dpt.id_897)"),
    dimensions: str = Query("", description="Dimensions séparées par virgule (optionnel)"),
    filters: str = Query("", description="Filtres: dimension:operator:value (ex: cube.commune:equals:Paris)"),
    limit: int = Query(100, ge=1, le=1000, description="Limite de résultats"),
    token: Optional[str] = Query(None, description="Token JWT")
):
    """
    Exécute une requête sur un cube d'indicateurs écologiques.
    Filtres supportés: equals, notEquals, contains, notContains, gt, gte, lt, lte
    """
    jwt_token = token or os.getenv("token")
    if not jwt_token:
        raise HTTPException(status_code=401, detail="Token JWT manquant")
    
    query = {
        "measures": [m.strip() for m in measures.split(",")],
        "limit": limit
    }
    if dimensions:
        query["dimensions"] = [d.strip() for d in dimensions.split(",")]
    
    if filters:
        query["filters"] = []
        for f in filters.split(";"):
            parts = f.split(":")
            if len(parts) >= 3:
                query["filters"].append({
                    "member": parts[0],
                    "operator": parts[1],
                    "values": [parts[2]]
                })
    
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = await client.get(
            f"{CUBEJS_API}/load",
            headers=headers,
            params={"query": str(query).replace("'", '"')},
            timeout=30.0
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text[:500])
        
        return response.json()


# ============ Endpoints utilitaires ============

@app.get("/")
async def root():
    """Point d'entrée de l'API avec documentation."""
    return {
        "message": "API Écologie France",
        "documentation": "/docs",
        "endpoints": {
            "tabular": {
                "description": "API Tabulaire data.gouv.fr (accès libre)",
                "base": "/tabular",
                "routes": [
                    "GET /tabular/resources/{resource_id}",
                    "GET /tabular/resources/{resource_id}/data",
                    "GET /tabular/resources/{resource_id}/profile"
                ]
            },
            "indicateurs": {
                "description": "Hub Indicateurs Transition Écologique (token JWT requis)",
                "base": "/indicateurs",
                "token_request": "https://grist.numerique.gouv.fr/o/ecolabservicesdonnees/forms/1d4wnsMrTwY8RaiU2WbjP8/47",
                "routes": [
                    "GET /indicateurs/cubes",
                    "GET /indicateurs/query?cube=...&measures=..."
                ]
            }
        },
        "example_resource_id": "1c5075ec-7ce1-49cb-ab89-94f507812daf"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

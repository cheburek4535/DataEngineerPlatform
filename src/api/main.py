from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import *

app = FastAPI(
    title="WeatherGuard API",
    description="Data Platform",
    version="0.1.0",
    swagger_ui_parameters={"defaultModelsExpandDepth": -1}
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(weather_router)
app.include_router(locations_router)
app.include_router(anomalies_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="127.0.0.1", port=8000, reload=True)
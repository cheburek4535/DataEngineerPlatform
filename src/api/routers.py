from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_
from services.db.db import get_db
import api.schemas as schemas
from services.weather.locations import add_location_to_track
from typing import List
from services.db import models

locations_router = APIRouter(prefix="/locations", tags=["Локации"])
anomalies_router = APIRouter(prefix="/anomalies", tags=["Аномалии"])
weather_router = APIRouter(prefix="/weather", tags=["Погода"])

@locations_router.post("/add", response_model=schemas.Location)
async def add_location(location: schemas.LocationCreate, db: Session=Depends(get_db)) -> schemas.Location:
    return schemas.Location.model_validate(add_location_to_track(db, **location.model_dump()))

@locations_router.get("/list", response_model=List[schemas.Location])
def get_locations_list(db: Session=Depends(get_db)) -> List[schemas.Location]:
    return [
        schemas.Location.model_validate(l)
        for l in db.query(models.LocationToTrack).all()
    ]
@locations_router.delete("/{loc_id}/delete")
def delete_location(loc_id: int, db: Session=Depends(get_db)) -> str:
    loc = db.query(models.LocationToTrack).filter(models.LocationToTrack.id == loc_id).first()
    if not loc:
        raise HTTPException(status_code=404, detail="Локация не найдена")
    db.query(models.LocationToTrack).filter_by(id=loc_id).delete()
    db.commit()
    return f"Локация {loc_id} удалена"

@anomalies_router.get("/list", response_model=List[schemas.Anomaly])
def get_anomalies_list(db: Session=Depends(get_db)) -> List[schemas.Anomaly]:
    return [
        schemas.Anomaly.model_validate(a)
        for a in db.query(models.Anomaly).all()
    ]

@anomalies_router.get("/by-coordinates", response_model=List[schemas.Anomaly])
def get_anomalies_by_coordinates(lat: float, lon: float, approximate: bool = True, radius: float = 1.0, db: Session=Depends(get_db)) -> List[schemas.Anomaly]:
    if not approximate:
        anomalies = db.query(models.Anomaly).filter_by(lat=lat, lon=lon).all()
    else:
        anomalies = db.query(models.Anomaly).filter(
            and_(
                models.Anomaly.lat >= lat - radius,
                models.Anomaly.lat <= lat + radius,
                models.Anomaly.lon >= lon - radius,
                models.Anomaly.lon <= lon + radius
            )
        ).all()
    if anomalies:
        return [
            schemas.Anomaly.model_validate(a)
            for a in anomalies
        ]
    return []

@weather_router.get("/raw", response_model=List[schemas.RawWeather])
def get_raw_weather(db: Session=Depends(get_db)) -> List[schemas.RawWeather]:
    return [
        schemas.RawWeather.model_validate(r)
        for r in db.query(models.RawWeather).all()
        ]

@weather_router.get("/structured", response_model=List[schemas.Weather])
def get_weather(db: Session=Depends(get_db)) -> List[schemas.Weather]:
    return db.query(models.Weather).all() or []

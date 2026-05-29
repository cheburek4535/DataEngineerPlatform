@echo off

docker exec -i weather_db psql -U weather_user -d weather_guard < database_dump3.sql

echo Дамп импортирован
pause
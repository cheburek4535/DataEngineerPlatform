@echo off

docker exec weather_db pg_dump -U weather_user -d weather_guard --data-only --inserts > database_dump3.sql

echo Дамп экспортирован
pause
@echo off
cls
echo Welcome to Kite Databases!
echo     Launching all database components...

:: Start JSON-based NoSQL database (Python) in new terminal
start "KiteDB NoSQL JSON" cmd /k "cd kitedb_nosql_json && python server.py"

:: Start Graph database (Go) in new terminal
start "User manager" cmd /k "cd user_management_system && npm run dev"

:: Start third database or any other service (example)
start "Backend" cmd /k "cd user_manager_backend && npm run server"

echo All services started in separate terminals.
pause

@echo off
cls
echo Welcome to Kite Databases!
echo     Launching all database components...

:: Start JSON-based NoSQL database (Python) in new terminal
echo Starting KiteDB...
start "KiteDB NoSQL JSON" cmd /k "cd kitedb_nosql_json && python server.py"
timeout /t 3 /nobreak >nul

:: Start Frontend (React) in new terminal
echo Starting Frontend...
start "User Manager" cmd /k "cd user_management_system && npm i && npm run dev"
timeout /t 3 /nobreak >nul

:: Start Backend (Node.js) in new terminal
echo Starting Backend...
start "Backend" cmd /k "cd user_manager_backend && npm i && npm run server"
timeout /t 3 /nobreak >nul

echo All services started in separate terminals.
pause
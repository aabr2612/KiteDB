@echo off
echo Welcome to Kite Databases!
echo     Please choose a database to use:
echo          1. NO SQL based json database
echo          2. Graph database
echo          3. Exit
set /p choice=Enter your choice (1-3): 

if "%choice%"=="1" (
    echo Starting Json database...
    cd kitedb_nosql_json
    python .\main.py
    if errorlevel 1 (
        echo Error: Failed to run Python project. Check the path or Python installation.
        pause
    )
) else if "%choice%"=="2" (
    echo Starting Graph database...
    cd go_project
    go run main.go
    if errorlevel 1 (
        echo Error: Failed to run Go project. Check the path or Go installation.
        pause
    )
    cd ..
) else if "%choice%"=="3" (
    echo Exiting...
    exit /b 0
) else (
    echo Invalid choice! Please select 1, 2, or 3.
    pause
)

echo.
echo Press any key to return to the menu...
pause >nul
cls
goto :eof
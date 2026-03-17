@echo off
echo === FarmTracker Setup ===
echo.
echo Step 1: Installing Flask...
pip install flask
echo.
echo Step 2: Creating database and demo data...
python seed.py
echo.
echo Step 3: Starting the app...
echo.
echo Open your browser at: http://localhost:5000
echo Login: admin@farm.mw / password123
echo Press Ctrl+C to stop the server
echo.
python app.py

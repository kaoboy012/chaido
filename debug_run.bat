@echo off
cd d %~dp0
echo === DANG KIEM TRA PYTHON ===
python --version
echo.
echo === DANG CHAY SCRIPT ===
python main_script.py
echo.
echo === KET THUC (Ma loi %errorlevel%) ===
pause
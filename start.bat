@echo off
REM MyTube — script de démarrage automatique (Windows)
REM Détecte automatiquement si le VPN doit être activé selon le fichier .env

REM Créer .env depuis .env.example si absent
if not exist .env (
    echo [MyTube] Fichier .env introuvable =^> copie depuis .env.example
    copy .env.example .env >nul
    echo [MyTube] Editez .env pour configurer (VPN, SESSION_SECRET...) puis relancez.
    pause
    exit /b 0
)

REM Lire PROXY_URL depuis .env (ignorer les lignes commentées)
set PROXY_URL=
for /f "tokens=1,* delims==" %%A in ('findstr /r "^PROXY_URL=" .env') do set PROXY_URL=%%B

if defined PROXY_URL (
    if not "!PROXY_URL!"=="" (
        echo [MyTube] VPN active (PROXY_URL=%PROXY_URL%)
        docker compose --profile vpn up -d --build
        goto done
    )
)

echo [MyTube] Demarrage sans VPN
docker compose up -d --build

:done
echo.
echo MyTube est disponible sur http://localhost:3000
pause

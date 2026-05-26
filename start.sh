#!/usr/bin/env bash
# MyTube — script de démarrage automatique
# Détecte automatiquement si le VPN doit être activé selon le fichier .env

set -e

# Créer .env depuis .env.example si absent
if [ ! -f .env ]; then
  echo "[MyTube] Fichier .env introuvable → copie depuis .env.example"
  cp .env.example .env
  echo "[MyTube] Éditez .env pour configurer (VPN, SESSION_SECRET…) puis relancez."
  exit 0
fi

# Lire PROXY_URL depuis .env (ignorer les lignes commentées)
PROXY=$(grep -E '^PROXY_URL=' .env | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)

if [ -n "$PROXY" ]; then
  echo "[MyTube] VPN activé (PROXY_URL=$PROXY)"
  docker compose --profile vpn up -d --build
else
  echo "[MyTube] Démarrage sans VPN"
  docker compose up -d --build
fi

echo ""
echo "✓ MyTube est disponible sur http://localhost:3000"

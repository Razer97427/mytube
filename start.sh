#!/usr/bin/env bash
# MyTube — script de démarrage automatique
# Détecte si le VPN doit être activé selon le fichier .env

set -e

# Créer .env depuis .env.example si absent
if [ ! -f .env ]; then
  echo "[MyTube] Fichier .env introuvable → copie depuis .env.example"
  cp .env.example .env
  echo "[MyTube] Éditez .env pour configurer (SESSION_SECRET, VPN, chemins…) puis relancez."
  exit 0
fi

# Charger les variables du .env (ignorer commentaires et lignes vides)
set -a
# shellcheck disable=SC1091
source <(grep -E '^[A-Z_]+=.' .env | grep -v '^#') 2>/dev/null || true
set +a

# Créer les dossiers de volumes personnalisés si définis
for VAR in CACHE_PATH POT_DATA_PATH; do
  VAL="${!VAR:-}"
  if [ -n "$VAL" ] && [ ! -d "$VAL" ]; then
    echo "[MyTube] Création du dossier $VAR : $VAL"
    mkdir -p "$VAL"
  fi
done

# Lire PROXY_URL depuis .env (ignorer les lignes commentées)
PROXY=$(grep -E '^PROXY_URL=.' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)

if [ -n "$PROXY" ]; then
  echo "[MyTube] VPN activé (PROXY_URL=$PROXY)"
  docker compose --profile vpn up -d --build
else
  echo "[MyTube] Démarrage sans VPN"
  docker compose up -d --build
fi

echo ""
echo "✓ MyTube disponible sur http://$(hostname -I | awk '{print $1}'):${FRONTEND_PORT:-3000}"

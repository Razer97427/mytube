# MyTube

Interface YouTube privée — sans pubs, sans tracking, avec SponsorBlock.
Fonctionne sur navigateur et s'installe comme une app native sur iOS/Android (PWA).

---

## Démarrage rapide

### Prérequis
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose

### Sans VPN (par défaut)

```bash
# Linux / macOS
./start.sh

# Windows
start.bat
```

Le script crée automatiquement un `.env` depuis `.env.example` si aucun fichier de configuration n'existe.
Accéder ensuite à **http://IP_SERVEUR:3000**

---

## Configuration VPN (optionnel)

Copier `.env.example` en `.env`, puis décommenter et remplir la section VPN souhaitée.

### Option 1 — NordVPN (ou autre fournisseur gluetun)

Récupérer les identifiants OpenVPN : **Dashboard NordVPN → Manual Setup → OpenVPN credentials** (ce ne sont PAS vos identifiants de compte NordVPN).

```env
VPN_PROVIDER=nordvpn
VPN_USER=your_service_username
VPN_PASS=your_service_password
VPN_COUNTRY=France
PROXY_URL=http://vpn:8888
```

### Option 2 — Fichier .ovpn custom

Placer le fichier dans `./vpn/client.ovpn` (créer le dossier `vpn/` si besoin).

```env
VPN_PROVIDER=custom
OVPN_FILE=./vpn/client.ovpn
# VPN_USER=...   # décommenter si le fichier .ovpn demande une authentification
# VPN_PASS=...
PROXY_URL=http://vpn:8888
```

### Démarrage avec VPN

Une fois le `.env` configuré, lancer le même script — il détecte automatiquement si le VPN doit être activé :

```bash
./start.sh    # ou start.bat sur Windows
```

---

## Connexion Google (optionnel)

MyTube supporte la connexion Google via le flux **Device Code** (comme SmartTube) : aucune configuration OAuth requise, aucune clé API à créer.

1. Cliquer sur **Se connecter** dans l'interface
2. Un code s'affiche — aller sur **youtube.com/activate** sur n'importe quel appareil
3. Entrer le code → la connexion s'effectue automatiquement

Fonctionnalités débloquées après connexion :
- Feed personnalisé (page d'accueil basée sur vos abonnements)
- Liste de vos abonnements dans la sidebar

---

## Fonctionnalités

| Fonctionnalité | Détail |
|---|---|
| Recherche YouTube | Résultats temps réel via yt-dlp |
| Tendances | Page tendances YouTube |
| Feed personnalisé | Nécessite connexion Google |
| Lecture vidéo | Proxy byte-range transparent, zéro pub |
| Qualité vidéo | 360p → 1080p sélectionnable |
| SponsorBlock | Saut automatique des segments publicitaires |
| Recommandations | Vidéos similaires + lecture automatique suivante |
| Abonnements | Sidebar avec vos chaînes (connexion Google requise) |
| Historique | Conservé localement, section "Continuer à regarder" |
| Picture-in-Picture | Fenêtre flottante, fonctionne sur iOS |
| Lecture en arrière-plan | Audio en veille / écran verrouillé (iOS + Android) |
| Contrôles écran verrouillé | Titre de la vidéo, miniature, boutons play/pause/suivant |
| PWA installable | iOS Safari → Partager → Sur l'écran d'accueil |
| VPN | NordVPN ou fichier .ovpn custom via gluetun |

---

## Architecture

```
[Navigateur / Safari iOS]
        ↓
[Frontend nginx :3000]
        ↓  /api/*  /auth/*
[Backend FastAPI :8080]
   ├── yt-dlp            (recherche, extraction, streaming)
   ├── pot-provider :4416 (po_token anti-bot automatique)
   ├── InnerTube API      (recommandations, tendances, feed)
   ├── SponsorBlock API   (segments à ignorer)
   └── Google OAuth2      (device code flow)
        ↓ (si VPN activé)
[gluetun VPN]
   └── proxy HTTP :8888   (NordVPN ou .ovpn custom)
```

Le streaming vidéo fonctionne en **proxy byte-range temps réel** : YouTube CDN → backend → navigateur par chunks de 64 Ko. La vidéo n'est jamais stockée sur le serveur.

---

## Installation PWA sur iOS

1. Ouvrir **http://IP_SERVEUR:3000** dans **Safari** (obligatoirement Safari)
2. Toucher l'icône **Partager** (carré avec flèche)
3. Sélectionner **"Sur l'écran d'accueil"**
4. MyTube s'installe comme une application native

---

## Ports

| Port | Service | Accès |
|---|---|---|
| 3000 | Frontend (accès principal) | Public |
| 8080 | Backend API | Interne (via nginx) |
| 4416 | pot-provider | Interne |
| 8888 | Proxy VPN gluetun | Interne (si VPN activé) |

---

## Mise à jour

```bash
# Mettre à jour toute la stack
docker compose pull
./start.sh

# Mettre à jour uniquement yt-dlp (sans rebuild)
docker compose exec backend pip install --upgrade yt-dlp
docker compose restart backend
```

---

## Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `SESSION_SECRET` | `changeme_in_production` | Clé de chiffrement des sessions — **changer en production** |
| `PROXY_URL` | *(vide)* | URL du proxy VPN, ex: `http://vpn:8888` |
| `VPN_PROVIDER` | `nordvpn` | Fournisseur gluetun (`nordvpn`, `custom`, etc.) |
| `VPN_USER` | *(vide)* | Identifiant OpenVPN du fournisseur |
| `VPN_PASS` | *(vide)* | Mot de passe OpenVPN du fournisseur |
| `VPN_COUNTRY` | `France` | Pays du serveur VPN |
| `OVPN_FILE` | *(vide)* | Chemin vers le fichier `.ovpn` (mode custom) |
| `POT_PROVIDER_URL` | `http://pot-provider:4416` | URL interne du pot-provider |
| `SPONSORBLOCK_API` | `https://sponsor.ajay.app` | URL de l'API SponsorBlock |

---

## Dépannage

**La vidéo ne se lance pas**
→ Le po_token est peut-être expiré :
```bash
docker compose restart pot-provider
```

**Erreur "Sign in to confirm you're not a bot"**
→ Reconstruire le pot-provider :
```bash
docker compose up -d --build pot-provider
```

**Le VPN ne se connecte pas**
→ Vérifier les logs gluetun :
```bash
docker compose --profile vpn logs vpn -f
```

**Recherche vide / erreurs backend**
```bash
docker compose logs backend -f
```

**Réinitialisation complète**
```bash
docker compose down -v
./start.sh
```

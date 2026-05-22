# MyTube 🎬

Interface YouTube privée — sans pubs, sans tracking, avec SponsorBlock.  
Proxy scraper basé sur yt-dlp. PWA installable sur iOS.

## Stack

| Service | Rôle |
|---|---|
| `yt-dlp` | Scraping YouTube + contournement blocages |
| `FastAPI` | API backend légère (Python) |
| `bgutil-ytdlp-pot-provider` | Génération automatique du po_token |
| `SponsorBlock` | Saut de segments automatique |
| `nginx` | Frontend PWA |

---

## Démarrage rapide

```bash
# 1. Cloner / dézipper le projet
cd mytube

# 2. Lancer tout en une commande
docker compose up -d --build

# 3. Accéder à l'interface
http://IP_SERVEUR:3000
```

---

## Installation iOS (PWA)

1. Ouvrir `http://IP_SERVEUR:3000` dans **Safari**
2. Bouton Partager → **"Sur l'écran d'accueil"**
3. MyTube s'installe comme une app native ✓

---

## Architecture

```
[Safari iOS / Navigateur]
        ↓
[Frontend nginx :3000]
        ↓ proxy /api/*
[Backend FastAPI :8080]
   ├── yt-dlp (search, stream)
   ├── pot-provider :4416 (po_token auto)
   └── SponsorBlock API publique
```

---

## Fonctionnalités

- ✅ Recherche YouTube
- ✅ Page tendances
- ✅ Lecture vidéo proxy (sans pubs)
- ✅ SponsorBlock (saut automatique des segments)
- ✅ Sélecteur de qualité (720p, 1080p…)
- ✅ PWA installable iOS/Android
- ✅ po_token automatique (renouvellement transparent)

---

## Mise à jour yt-dlp

YouTube change régulièrement ses protections.  
Pour mettre à jour yt-dlp sans rebuilder toute l'image :

```bash
docker compose exec backend pip install --upgrade yt-dlp
```

---

## Ports utilisés

| Port | Service |
|---|---|
| 3000 | Frontend (accès principal) |
| 8080 | Backend API (interne) |
| 4416 | pot-provider (interne) |

---

## Dépannage

**Vidéo ne se lance pas**  
→ Le po_token est peut-être expiré. Redémarrer le pot-provider :
```bash
docker compose restart pot-provider
```

**Recherche vide**  
→ Vérifier les logs backend :
```bash
docker compose logs backend -f
```

**Mise à jour complète**
```bash
docker compose pull
docker compose up -d --build
```

# ETL Toolbox de NRCAN


[![codecov](https://codecov.io/github/xmalet-nrcan/xm-etl-toolbox/graph/badge.svg?token=P4ISY9JL78)](https://codecov.io/github/xmalet-nrcan/xm-etl-toolbox)
[![CI](https://github.com/xmalet-nrcan/xm-etl-toolbox/actions/workflows/ci-release.yml/badge.svg)](https://github.com/xmalet-nrcan/xm-etl-toolbox/actions/workflows/ci-release.yml)

`etl-toolbox` est une suite d'outils Python conçue pour simplifier les processus d'extraction, transformation et chargement (ETL) de données. Cette boîte à outils modulaire offre plusieurs composants spécialisés pour différents aspects des flux de travail ETL.

## Composants

### etl_logging
Module de journalisation spécialisé pour les processus ETL, permettant une configuration simple et une analyse efficace des logs.

### etl_toolbox
Collection d'outils pour lire des données à partir de diverses sources. Il inclut des lecteurs pour différents formats de fichiers et bases de données, facilitant l'intégration des données dans les processus ETL.:
- **Lecteurs de données**: CSV, Excel, GeoPackage, JSON, PostGIS, Shapefile


### database
Interfaces et ORM pour interagir avec différents systèmes de bases de données:
- **Interfaces de base de données**: Gestionnaires d'objets abstraits pour les interactions avec les bases de données
- **ORM**: Mappages objet-relationnel pour simplifier l'accès aux données

## Installation

Installez le package via Poetry:

```bash
poetry install
```

Ou en créant une distribution:

```bash
poetry build
pip install dist/nrcan_etl_toolbox-*.whl
```

## Utilisation

### Module de journalisation (etl_logging)

```python
from nrcan_etl_toolbox.etl_logging import CustomLogger

logger = CustomLogger(level='INFO'
                      ,logger_type='verbose',
                      logger_file_name='test_logger.log')

# Journalisation des messages
logger.info("Début du processus ETL")
logger.debug("Détails techniques", extra={"data": {"items": 100}})
logger.error("Erreur de traitement", exc_info=True)
```

### Lecteurs de données (etl_toolbox)

```python
from nrcan_etl_toolbox.etl_toolbox.reader import ReaderFactory

# Création d'un lecteur CSV
csv_reader = ReaderFactory(input_source="donnees.csv")
data = csv_reader.data

# Création d'un lecteur Shapefile
shp_reader = ReaderFactory(input_source="donnees.shp")
geo_data = shp_reader.data
```

### Interface de base de données

```python
# TODO : Finaliser documentation.
from nrcan_etl_toolbox.database.interface import AbstractDatabaseHandler
# Exemple d'utilisation à documenter
```

## Développement

Pour contribuer au projet, installez les dépendances de développement:

```bash
poetry install --with dev
```

Exécutez les tests avec:

```bash
pytest
```

## Structure du projet

```
nrcan_etl_toolbox/
├── database/               # Interactions avec les bases de données
│   ├── interface/          # Interfaces abstraites pour les bases
│   └── orm/                # Mappages objet-relationnel
├── etl_logging/            # Module de journalisation ETL
└── etl_toolbox/            # Outils principaux ETL
    └── reader/             # Lecteurs de sources de données
        └── source_readers/ # Implémentations spécifiques des lecteurs
```

[//]: # (## Licence)

[//]: # ()
[//]: # (Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE]&#40;LICENSE&#41; pour plus d'informations.)

## Auteurs

- NRCAN (Ressources Naturelles Canada)
- [Xavier Malet](mailto:xavier.malet@nrcan-rncan.gc.ca)

Pour toute question ou suggestion, veuillez utiliser les issues GitHub du projet.
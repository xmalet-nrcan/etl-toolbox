# ETL-LOGGING

[![codecov](https://codecov.io/gh/xmalet-nrcan/xm-etl-toolbox/branch/main/graph/badge.svg)](https://codecov.io/gh/xmalet-nrcan/xm-etl-toolbox)

[![CI](https://github.com/xmalet-nrcan/xm-etl-toolbox/actions/workflows/ci-release.yml/badge.svg)](https://github.com/xmalet-nrcan/xm-etl-toolbox/actions/workflows/ci-release.yml)

`etl-logging` est un package Python destiné à faciliter la gestion du logging dans des processus ETL. Il offre des fonctionnalités simples et modulables pour configurer, écrire et analyser les logs de vos applications.

## Installation

Vous pouvez installer elt-logging via Poetry :

```bash
poetry install
```

Ou en créant une distribution avec Poetry :

```bash
poetry build
pip install dist/elt_logging-0.1.0-py3-none-any.whl
```

## Utilisation

Voici un exemple d'utilisation de base :

```python
from elt_logging import logger

# Configuration du logger (à adapter selon vos besoins)
logger.configure(level="INFO")

# Enregistrement d'un message
logger.info("Lancement du processus ETL")
```

## Configuration

Vous pouvez adapter la configuration du logger selon les spécificités de votre projet. Consultez la documentation interne pour en savoir plus sur les options disponibles.

## Développement

Pour contribuer au projet, installez les dépendances de développement :

```bash
poetry install --with dev
```

Ensuite, vous pouvez lancer les tests avec :

```bash
pytest
```

## License

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus d'informations.

## Auteurs

- Votre Nom (votre.email@example.com)

N'hésitez pas à contribuer et à signaler des bugs ou des améliorations via le dépôt GitLab/GitHub du projet.
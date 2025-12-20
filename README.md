Step 1: Install postgres with Docker Compose

```bash

docker compose -f compose_postgres.yml up -d
```

Step 2: Initialize Astro project and run

```bash
brew install astro
# If you haven't initialized an Astro project yet, run:
# astro dev init
```

Step 3: Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

Step 4: Run Astro project

```bash
astro dev start
```

Step 5: Access Airflow UI

Open your web browser and navigate to http://localhost:8080 to access the Airflow UI.

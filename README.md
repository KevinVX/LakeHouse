Step 1: Install postgres with Docker Compose

```bash

docker compose -f compose_postgres.yml up -d
```

Step 2: Initialize Astro project and run

```bash
astro dev init
```

Step 3: Clone the jaffle shop repo into the dags folder

```bash
cd dags/
git clone https://github.com/dbt-labs/jaffle-shop-classic.git
```

Step 4: Run Astro project

```bash
astro dev run
```

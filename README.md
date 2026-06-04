# Movie History Pipeline · Orchestration (Azure Data Factory)

This repository contains the Azure Data Factory configuration that schedules and governs the Movie History data pipeline. It handles everything *around* the computation: when to run, whether the data is ready, in what order to execute, and who to notify if something goes wrong.

The computation layer — Databricks notebooks, Delta Lake transformations, and the Medallion Architecture — lives in the companion repository: [Databricks_movie_history](https://github.com/JohanRROT/Databricks_movie_history).

---

## What ADF Does Here

Azure Data Factory plays a specific, deliberate role in this system. It does not transform data — that is Databricks' job. What ADF provides is:

- **Scheduling** — a weekly trigger fires the pipeline without manual intervention
- **Dependency management** — transformation only starts after ingestion succeeds, enforced at the pipeline level
- **Pre-flight validation** — a `GetMetadata` check confirms the source data exists before spinning up any compute
- **Failure notification** — if source data is missing, an alert fires immediately rather than letting the pipeline run and silently produce empty results
- **Parameter propagation** — `p_file_date` and `p_environment` are injected once at the trigger and flow through every activity down to each Databricks notebook

---

## Pipeline Topology

The orchestration is composed of three pipelines and one trigger.

```
tg_proccess_movie_history  (Weekly · Every Monday 14:40 UTC)
            │
            ▼
pl_proccess_movie_history  (Main orchestrator)
    │
    ├──▶ pl_inges_movie_history_data          [wait for completion]
    │         ├─ GetMetadata: check Bronze folder exists
    │         ├─ IF false → email alert notebook
    │         └─ IF true  → 13 Databricks notebook activities (partially parallel)
    │
    └──▶ pl_transformation_movie_history_data  [depends on: ingestion Succeeded]
              ├─ GetMetadata: check Bronze folder exists
              ├─ IF false → email alert notebook
              └─ IF true  → 4 Databricks notebook activities (fully parallel)
```

---

## Pipelines

### `pl_proccess_movie_history` — Main Orchestrator

The single entry point for a full pipeline run. Accepts two parameters and passes them downstream:

| Parameter | Type | Default |
|---|---|---|
| `p_file_date` | string | — |
| `p_environment` | string | `Production` |

Contains two activities:

1. **Execute Ingestion** — calls `pl_inges_movie_history_data`, waits for completion
2. **Execute Transformation** — calls `pl_transformation_movie_history_data`, waits for completion; **only runs if Execute Ingestion succeeded**

---

### `pl_inges_movie_history_data` — Ingestion Pipeline

Loads raw Bronze data into the Silver layer via 13 Databricks notebooks.

**Guard activity — `GetMetadata`**  
Checks `exists` on the Bronze folder for the given `p_file_date` before any notebook runs. If the folder is missing, the pipeline branches to an email alert and stops. This prevents the cluster from starting for nothing and ensures no empty partitions are written to Silver.

**Notebook execution — partially parallel**  
The 13 ingestion notebooks have data dependencies between them (e.g. `movies_languages` requires both `movies` and `languages` to exist in Silver first). ADF models these as activity dependencies, enabling parallel execution where possible:

```
Group A                      Group B                   Group C              Group D
────────────────────         ─────────────────────     ────────────         ────────────
Ingest Movie File            Ingest Language File       Ingest Movie         Ingest Genre
        │                            │                  Cast File            File
        ▼                            ▼                        │                  │
Ingest Language Role File    Ingest Prod Company File   Ingest Movie         Ingest Country
        │                            │                  Company File         File
        ▼                            ▼                        │                  │
Ingest Movie Language File   Ingest Prod Country File   Ingest Movie         Ingest Person
                                                        Genre File           File
```

All notebooks receive:
- `p_environment` → `@pipeline().parameters.p_environment`
- `p_file_date` → `@formatDateTime(pipeline().parameters.p_file_date, 'yyyy-MM-dd')`

---

### `pl_transformation_movie_history_data` — Transformation Pipeline

Joins and aggregates Silver tables into four Gold tables via Databricks notebooks.

**Guard activity — `GetMetadata`**  
Same Bronze folder existence check as the ingestion pipeline.

**Notebook execution — fully parallel**  
All four transformation notebooks read from independent Silver tables and write to independent Gold tables. ADF runs them simultaneously with no inter-dependencies:

| Activity | Notebook | Gold Output |
|---|---|---|
| Transformation results movie genre language | `01.results_movie_genre_language` | `result_movie_genre_language` |
| Transformation results country prod company | `02.results_country_prod_company` | `result_country_prod_company` |
| Transformation group movie | `03.result_group_movie` | `result_group_movie_genre` |
| Transformation group movie country | `04.result_group_movie_country` | `result_group_movie_country` |

All notebooks receive:
- `p_file_date` → `@formatDateTime(pipeline().parameters.p_file_date, 'yyyy-MM-dd')`

---

## Trigger

**`tg_proccess_movie_history`** — Schedule trigger  
Fires `pl_proccess_movie_history` every Monday at 14:40 UTC.

```json
"recurrence": {
    "frequency": "Week",
    "interval": 1
}
```

Parameters passed at trigger time:
- `p_environment`: `"Production"`
- `p_file_date`: target date to process (format: `yyyy-MM-dd`)

To run for a different date, trigger `pl_proccess_movie_history` manually from the ADF UI or via the ADF REST API.

---

## Linked Services

### `LS_Databricks` — Azure Databricks
- **Authentication**: Managed Service Identity (no credentials stored in ADF)
- **Workspace**: `adb-3400529298850403.3.azuredatabricks.net`
- **Cluster**: existing cluster (avoids cold-start overhead on each run)

### `ls_movie_history_adls` — Azure Data Lake Storage Gen2
- **Endpoint**: `moviehistorycol.dfs.core.windows.net`
- **Authentication**: Encrypted credential (ADF-managed)
- **Used by**: `GetMetadata` activities to check Bronze folder existence

---

## Repository Structure

```
adf-movie-history/
├── pipeline/
│   ├── pl_proccess_movie_history.json              # Main orchestrator
│   ├── pl_inges_movie_history_data.json            # 13-notebook ingestion pipeline
│   └── pl_transformation_movie_history_data.json   # 4-notebook parallel transformation
├── trigger/
│   └── tg_proccess_movie_history.json              # Weekly schedule trigger
├── linkedService/
│   ├── LS_Databricks.json                          # Databricks connection (MSI)
│   └── ls_movie_history_adls.json                  # ADLS Gen2 connection
├── dataset/
│   └── ds_movie_history_bonze.json                 # Bronze dataset (parameterised by date)
└── factory/
    └── databricks-uc-moviegistory-ext-access.json  # Factory metadata
```

---

## Related Repository

The Databricks notebooks, Delta Lake schema definitions, PySpark transformations, and Medallion Architecture implementation are maintained separately:

**[JohanRROT/Databricks_movie_history](https://github.com/JohanRROT/Databricks_movie_history)**

---

## License

Released under the [MIT License](LICENSE).

---

## Author

**Johan Rodriguez** · Data Engineer  
[LinkedIn](https://linkedin.com/in/) · [GitHub](https://github.com/JohanRROT)

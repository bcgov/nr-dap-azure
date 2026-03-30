# Fabric Lakehouse Medallion QuickStart

This QuickStart provides a ready-to-use implementation of the Medallion (Bronze–Silver–Gold) Lakehouse architecture in Microsoft Fabric, designed to help clients rapidly deploy a scalable data lakehouse solution.

## Quick Start

**New to this project?** → Start with **[SETUP.md](SETUP.md)** for detailed step-by-step instructions to get up and running in Fabric.

### Fabric Git Quickstart (connect → sync → run → disconnect)

Use this 5‑minute path to pull the notebook into a new, empty workspace and import the training files.

1. **Create** a new empty Fabric workspace.
2. **Connect the workspace to GitHub**  
   - **Workspace settings → Git integration**  
   - Provider: GitHub  
   - Repo: `bcgov/nr-dap-fabric-quickstart`  
   - Branch: `main`  
3. **Initial sync**: choose **Git → Workspace** (your workspace is empty).
4. **Run the notebook** `bootstrap/01_import_files_root` → **Run all**  
   - Creates/attaches Lakehouse **`quickstart_lh`**  
   - Copies **text assets** from the branch root into **Lakehouse → Files → `quickstart`**  
   - (Binary files are skipped by default; see SETUP for how to enable them.)
5. **Disconnect Git**  
   - **Workspace settings → Git integration → Disconnect**  
   - Prevents accidental commits back to the repo; your items remain in the workspace.

> **Tip:** If your organization restricts outbound traffic, allow `api.github.com` and `raw.githubusercontent.com` for the one‑time import.  
> **Note:** Lakehouse data (Tables and Files) isn’t tracked in Git; the notebook places assets locally for each learner.

## Project Structure

```
fabric-medallion-quickstart/
├── SETUP.md                          # 👈 START HERE - Detailed setup guide
├── README.md                         # This file - project overview
├── .github                           # CODEOWNERS CODEOWNERS and any workflows
├── bootstrap                         # Fabric‑committed notebook(s) + a lightweight README
│   ├── 01_import_files_root.Notebook # Fabric representation of the notebook item
│   │   ├── .platform                 # Fabric generated platform file
│   │   └── notebook-contents.py      # source (cells + metadata)
│   └── README.md                     # instructions (connect → sync → run → disconnect)
├── docs/                             # Architecture and design documentation
│   ├── architecture.md               # Architecture decisions and patterns
│   └── naming-conventions.md         # Naming standards for Fabric items
├── notebooks/                        # PySpark notebooks for data processing
│   ├── bronze.py                     # Bronze layer: raw data ingestion
│   ├── silver.py                     # Silver layer: data cleansing & conformance
│   └── gold.py                       # Gold layer: business marts & aggregations
├── dq/                               # Data quality validation
│   └── dq_checks.py                  # Quality checks for curated data
├── schemas/                          # SQL schema definitions (for future Warehouse use)
│   ├── create_replication_schema.sql # Create <source>_replication schemas
│   ├── create_reporting_schema.sql   # Create <source>_reporting schemas
│   └── seed_security_examples.sql    # Column-level and row-level security examples
├── samples/                          # Sample data for testing
│   └── customers.csv                 # Sample customer data (6 rows)
├── templates/                        # Copy activity templates for data ingestion
│   ├── copy_oracle.json              # Oracle → Lakehouse Bronze
│   ├── copy_postgresql.json          # PostgreSQL → Lakehouse Bronze
│   ├── copy_s3.json                  # S3 → Lakehouse Bronze
│   ├── copy_api.json                 # REST API → Lakehouse Bronze
│   └── copy_dataverse.json           # Dataverse → Lakehouse Bronze
└── pipeline/                         # Pipeline orchestration
    └── pipeline.json                 # Sample orchestration (reference only)
```

## Architecture Overview

### Medallion Zones

This QuickStart implements a three-tier medallion architecture:

- **Bronze (Replication)** → `<source>_replication` schema
  - Raw data landing zone
  - Reflects exactly what is in the source system
  - Schema-on-read
  - Example: `erp_replication.customers_raw`

- **Silver (Transformation)** → `<source>_staging` schema and/or `<source>_history` schema
  - Cleansed, validated, and conformed data
  - Transformation may include type conversions, deduplication, standardization
  - Collecting table history would also be part of Silver layer
  - Example: `erp_staging.customers_curated` or `erp_history.customers_monthly`

- **Gold (Reporting)** → `<source>_reporting` schema
  - Consumable data layer
  - Business-ready aggregations and metrics
  - Optimized for analytics and reporting
  - Example: `erp_reporting.customer_country_ageband_mart`

### Key Features

- **Delta Lake**: All tables use Delta format for ACID transactions, time travel, and efficient merges
- **Parameterized**: Single `source` parameter controls schema routing
- **Source-Scoped Schemas**: Keep data from different sources logically separated
- **Data Quality**: Built-in validation checks between layers
- **Extensible**: Template-based approach for adding new data sources

## Standard Schemas & Zone Routing

All processing uses **source-scoped schemas** to maintain clear data lineage:

| Zone | Schema Pattern | Purpose | Write Mode |
|------|---------------|---------|------------|
| Bronze | `<source>_replication` | Raw replicated data | Append |
| Silver | `<source>_reporting` | Cleansed conformed data | Overwrite |
| Gold | `<source>_reporting` | Business marts | Overwrite |

**Examples:**
- `erp_replication.customers_raw` (Bronze)
- `erp_reporting.customers_curated` (Silver)
- `crm_replication.accounts_raw` (Bronze)
- `crm_reporting.accounts_curated` (Silver)

### About the SQL Schema Scripts

The `schemas/` folder contains SQL scripts for schema creation. **These scripts are not used in this Lakehouse QuickStart** but are provided for future Warehouse-based implementations where:
- Schemas can be created via T-SQL `CREATE SCHEMA` statements
- Column-level security (CLS) and row-level security (RLS) can be implemented
- SQL-based schema management is preferred

For this Lakehouse QuickStart, schemas are created manually in the Lakehouse UI (see [SETUP.md](SETUP.md) Step 2).

## Getting Started

### Prerequisites
- Microsoft Fabric workspace access (Contributor or Admin)
- Permissions to create Lakehouses and Pipelines
- Basic understanding of PySpark and SQL

### Installation Steps

**👉 See [SETUP.md](SETUP.md) for detailed instructions**

**Note**: This QuickStart uses Lakehouse only. Schemas must be created manually in the Lakehouse UI before running notebooks. The SQL scripts in `schemas/` folder are provided for future Warehouse-based implementations.

## Extending the QuickStart

### Adding New Data Sources

The QuickStart uses a parameterized `source` variable to support multiple data systems simultaneously.

#### Understanding Source Parameters

**What is ERP?** In this context, **ERP** stands for **Enterprise Resource Planning** systems (SAP, Oracle ERP, Microsoft Dynamics) - used as the default example source.

The `source` parameter controls schema routing throughout the medallion architecture:
- `source = 'erp'` → writes to `erp_replication` (Bronze) and `erp_reporting` (Silver/Gold)
- `source = 'crm'` → writes to `crm_replication` (Bronze) and `crm_reporting` (Silver/Gold)

#### Common Source System Patterns

| Source | Use Case | Example Systems | Schemas Created |
|--------|----------|-----------------|-----------------|
| `erp` | Enterprise Resource Planning | SAP, Oracle ERP, Dynamics 365 F&O | `erp_replication`, `erp_reporting` |
| `crm` | Customer Relationship Mgmt | Salesforce, Dynamics CRM, HubSpot | `crm_replication`, `crm_reporting` |
| `mkt` | Marketing Platforms | Marketo, HubSpot, Adobe Campaign | `mkt_replication`, `mkt_reporting` |
| `hr` | Human Resources | Workday, ADP, BambooHR | `hr_replication`, `hr_reporting` |
| `iot` | IoT/Sensor Data | Azure IoT Hub, AWS IoT | `iot_replication`, `iot_reporting` |
| `pos` | Point of Sale | Square, Toast, Shopify POS | `pos_replication`, `pos_reporting` |
| `fin` | Financial Systems | NetSuite, QuickBooks, Xero | `fin_replication`, `fin_reporting` |

#### Steps to Add a New Source

1. **Create source-specific schemas in Lakehouse UI**:
   - Open your Lakehouse
   - Right-click on **Schemas** → **New schema**
   - Create `crm_replication` and `crm_reporting` schemas

2. **Use a copy template** from `templates/`:
   - Oracle: `copy_oracle.json`
   - PostgreSQL: `copy_postgresql.json`
   - S3: `copy_s3.json`
   - REST API: `copy_api.json`
   - Dataverse: `copy_dataverse.json`

3. **Update Bronze notebook** to read from new source (or create source-specific notebook)

4. **Run pipeline** with `source` parameter set to new source name (e.g., `crm`)

**Result**: All data flows through the same medallion pattern with clear source isolation:
```
erp_replication → erp_reporting (ERP data flow)
crm_replication → crm_reporting (CRM data flow)
mkt_replication → mkt_reporting (Marketing data flow)
```

## Best Practices

### Data Quality
- Run DQ checks after each layer transformation
- Implement both structural and business rule validations
- Log failures for investigation and remediation

### Performance
- Partition large tables by date/region
- Use Z-ordering on frequently filtered columns
- Compact Delta tables regularly (`OPTIMIZE`)

### Security
- Apply column-level security in Warehouse (see `schemas/seed_security_examples.sql`)
- Use row-level security for multi-tenant scenarios
- Implement sensitivity labels for PII/PHI data

### Monitoring
- Track pipeline run durations and row counts
- Set up alerts for failures and data quality issues
- Create operational dashboards for data ops teams

## Documentation

- **[SETUP.md](SETUP.md)** - Step-by-step setup and testing guide
- **[architecture.md](docs/architecture.md)** - Architecture decisions and patterns
- **[naming-conventions.md](docs/naming-conventions.md)** - Naming standards for Fabric items

## Common Use Cases

### 1. ERP Integration (Oracle/SAP)
Use `templates/copy_oracle.json` + incremental loading by `LAST_UPDATE_DATE`

### 2. CRM Replication (Dataverse/Salesforce)
Use `templates/copy_dataverse.json` or API template for Salesforce

### 3. Cloud Data Lake (S3/ADLS)
Use `templates/copy_s3.json` with file pattern matching

### 4. IoT/Streaming Data
Extend Bronze notebooks to use Event Hub or Kafka sources

### 5. SaaS Application Data
Use `templates/copy_api.json` with pagination support

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Schema not found | Create schemas manually in Lakehouse UI (right-click Schemas → New schema) |
| Parameter not recognized | Verify notebooks attached to correct Lakehouse |
| DQ checks fail | Review Silver data quality, check for nulls/duplicates |
| Pipeline failure | Check notebook execution logs for detailed errors |

For detailed troubleshooting, see [SETUP.md](SETUP.md#troubleshooting).

## Support & Resources

- **Microsoft Fabric Documentation**: https://learn.microsoft.com/fabric/
- **Delta Lake Guide**: https://delta.io/
- **Fabric Community**: https://community.fabric.microsoft.com/

## Contributing

This is a template/quickstart project. Feel free to:
- Adapt naming conventions for your organization
- Add source-specific transformations
- Extend with additional data quality rules
- Implement custom security policies

## License

This template is provided as-is for use with Microsoft Fabric. Adapt as needed for your organization's requirements.

---

**Ready to get started?** → Open **[SETUP.md](SETUP.md)** and follow the step-by-step guide! 🚀

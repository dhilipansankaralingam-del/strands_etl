# Quick Start - Config-Driven ETL Framework

Get started with the Strands config-driven ETL framework in **2 ways**:

---

## 🚀 Option 1: Automated Setup (Recommended - 5 minutes)

Run the automated setup script:

```bash
./quick_start.sh
```

This script will:
- ✅ Check prerequisites (Python, AWS CLI, credentials)
- ✅ Create virtual environment
- ✅ Install all dependencies
- ✅ Set up AWS resources (S3, Glue database, IAM role)
- ✅ Update configuration with your settings
- ✅ Create sample data (10,000 transactions, 1,000 customers, 100 products)
- ✅ Validate the setup

**That's it!** The framework is ready to use.

---

## 📚 Option 2: Manual Setup (Step-by-Step)

Follow the detailed guide for a deeper understanding:

```bash
cat SETUP_GUIDE.md
```

or view it in your favorite markdown viewer.

The manual guide includes:
- Prerequisites checklist
- Step-by-step AWS setup
- Configuration explanations
- Sample data creation
- Running your first job
- Troubleshooting common issues

---

## ✅ What You Get

After setup (either option), you'll have:

1. **Configured Framework**
   - `etl_config.json` updated with your AWS settings
   - All dependencies installed
   - Virtual environment ready

2. **AWS Resources**
   - S3 bucket for data storage
   - Glue database and tables
   - IAM role for Glue jobs

3. **Sample Data**
   - 10,000 transaction records
   - 1,000 customer records
   - 100 product records
   - All registered in Glue Catalog

4. **Ready to Run**
   - Examples validated
   - Configuration tested
   - Auto-detection working

---

## 🎯 Next Steps

### 1. Run Examples

```bash
# Activate virtual environment (if not already active)
source venv/bin/activate

# Run configuration validation
python example_config_driven_usage.py
```

### 2. Test Auto-Detection

```bash
# Test auto-detection of table sizes
python -c "
from strands_agents.tools.catalog_tools import detect_glue_table_size

result = detect_glue_table_size(
    database='etl_demo_db',
    table='fact_transactions'
)

print(f'Table: {result[\"table\"]}')
print(f'Size: {result[\"size_gb\"]} GB')
print(f'Files: {result[\"num_files\"]}')
print(f'Broadcast eligible: {result[\"broadcast_eligible\"]}')
"
```

### 3. Run a Job

```bash
# Load and run a job from config
python -c "
from strands_agents.orchestrator.config_driven_orchestrator import ConfigDrivenOrchestrator

orchestrator = ConfigDrivenOrchestrator('./etl_config.json')

# List available jobs
loader = orchestrator.config_loader
jobs = loader.list_enabled_jobs()

print('Available jobs:')
for job in jobs:
    print(f'  - {job[\"job_id\"]}: {job[\"job_name\"]}')
"
```

### 4. Customize Configuration

Edit `etl_config.json` to add your own jobs:

```json
{
  "job_id": "my_custom_job",
  "job_name": "My Custom ETL",
  "enabled": true,
  "data_sources": [
    {
      "name": "my_table",
      "source_type": "glue_catalog",
      "database": "my_db",
      "table": "my_table",
      "auto_detect_size": true
    }
  ]
}
```

---

## 📖 Documentation

- **SETUP_GUIDE.md** - Detailed step-by-step setup instructions
- **CONFIG_DRIVEN_README.md** - Complete framework documentation
- **STRANDS_NATIVE_README.md** - Agent and tool reference
- **example_config_driven_usage.py** - 6 code examples

---

## 💡 Key Features

### 1. Table Name Support
Specify table names from multiple sources:
```json
{
  "source_type": "glue_catalog",
  "database": "analytics_db",
  "table": "fact_transactions",
  "auto_detect_size": true
}
```

### 2. Auto-Detection
Agents automatically detect:
- Table sizes (queries Glue Catalog, S3, Redshift, RDS)
- Partition counts
- File counts and formats
- Broadcast eligibility

### 3. Data Flow Analysis
Agents analyze how tables are used:
- Fact vs dimension classification
- Join relationships
- Optimal join order
- Broadcast recommendations

### 4. User Preference + Agent Recommendation
```json
{
  "platform": {
    "user_preference": "glue",
    "allow_agent_override": true
  }
}
```
- You specify preference
- Agent still analyzes and recommends
- You decide whether to allow override

### 5. Quality from Config
All validation rules in configuration:
```json
{
  "data_quality": {
    "completeness_checks": [...],
    "accuracy_rules": [...],
    "duplicate_check": {...}
  }
}
```

---

## 🆘 Troubleshooting

### Virtual environment not activated?
```bash
source venv/bin/activate
```

### AWS credentials not configured?
```bash
aws configure
```

### Missing dependencies?
```bash
pip install -r requirements.txt
```

### Need help?
See the **Troubleshooting** section in `SETUP_GUIDE.md`

---

## 🎓 Learning Path

1. **Start Here**: Run `./quick_start.sh`
2. **Understand**: Read `SETUP_GUIDE.md`
3. **Explore**: Run `example_config_driven_usage.py`
4. **Customize**: Edit `etl_config.json`
5. **Master**: Read `CONFIG_DRIVEN_README.md`

---

## ⏱️ Time Estimates

- **Automated Setup**: 5-10 minutes
- **Manual Setup**: 30-45 minutes
- **Running Examples**: 5 minutes
- **First Custom Job**: 15-30 minutes

---

## 🚀 Ready to Go?

```bash
# Option 1: Automated (quick)
./quick_start.sh

# Option 2: Manual (detailed)
cat SETUP_GUIDE.md
```

Happy ETL-ing! 🎉

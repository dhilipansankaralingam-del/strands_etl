# Python Version Compatibility Fix

## The Issue

You're seeing this error:
```
Requires-Python >=3.10
```

This means the **Strands Agents SDK** requires Python 3.10 or higher.

---

## Quick Check - What Python Version Do You Have?

```bash
python3 --version
```

**Results:**
- ✅ **Python 3.10+**: You're good! The error might be something else
- ⚠️ **Python 3.8 or 3.9**: You need to upgrade OR use the workaround below
- ❌ **Python 3.7 or lower**: You must upgrade

---

## ✅ Solution 1: Upgrade to Python 3.10+ (Recommended)

### Ubuntu/Debian (WSL included)

```bash
# Install Python 3.10
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-dev

# Verify
python3.10 --version
```

### Ubuntu/Debian - Alternative (using deadsnakes PPA)

```bash
# Add deadsnakes PPA for latest Python versions
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

# Install Python 3.11 (even better!)
sudo apt install python3.11 python3.11-venv python3.11-dev

# Verify
python3.11 --version
```

### macOS (Homebrew)

```bash
# Install Python 3.11
brew install python@3.11

# Verify
python3.11 --version
```

### macOS (pyenv - Recommended for managing multiple versions)

```bash
# Install pyenv
brew install pyenv

# Install Python 3.11
pyenv install 3.11.7

# Set as global default
pyenv global 3.11.7

# Verify
python --version
```

### Windows

1. Download Python 3.11+ installer from: https://www.python.org/downloads/
2. Run installer
3. ✅ Check **"Add Python to PATH"**
4. Choose "Install Now"
5. Verify in Command Prompt:
   ```
   python --version
   ```

### After Installing Python 3.10+

```bash
# Navigate to project
cd strands_etl

# Remove old virtual environment
rm -rf venv

# Create new venv with Python 3.10+
python3.10 -m venv venv
# or
python3.11 -m venv venv

# Activate
source venv/bin/activate

# Verify Python version in venv
python --version
# Should show: Python 3.10.x or 3.11.x

# Upgrade pip
pip install --upgrade pip

# Install requirements (should work now!)
pip install -r requirements.txt
```

---

## 🔧 Solution 2: Use Without Strands SDK (Workaround for Python 3.8/3.9)

If you can't upgrade Python right now, you can still use the config-driven framework with limited functionality.

### Install Compatible Dependencies

```bash
# Use the Python 3.8 compatible requirements
pip install -r requirements_py38.txt
```

### What Works Without Strands SDK?

✅ **Still Works:**
- AWS S3 operations
- AWS Glue catalog queries
- Data creation and upload
- Configuration loading
- Auto-detection of table sizes
- Data flow analysis (basic)
- All boto3-based AWS operations

❌ **Won't Work:**
- Native Strands agents (Decision, Quality, Optimization, etc.)
- Agent-based recommendations
- Swarm orchestration
- Learning from execution history

### Use AWS Bedrock Agents Instead

If you can't use Strands SDK, you can use AWS Bedrock Agents for agentic capabilities:

```bash
# All these still work
aws bedrock list-foundation-models
aws bedrock-agent create-agent

# Use Bedrock SDK instead
pip install boto3 anthropic
```

---

## 🎯 Solution 3: Use Docker (Isolates Python Version)

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy project files
COPY . .

CMD ["/bin/bash"]
```

Build and run:

```bash
# Build Docker image
docker build -t strands-etl .

# Run container
docker run -it \
  -v ~/.aws:/root/.aws \
  -v $(pwd):/app \
  strands-etl

# Inside container, Python 3.11 is available
python --version
```

---

## 📊 Comparison: Which Solution?

| Solution | Time | Complexity | Functionality |
|----------|------|------------|---------------|
| **Upgrade Python** | 10 min | Easy | ✅ Full (100%) |
| **Use requirements_py38.txt** | 2 min | Very Easy | ⚠️ Limited (60%) |
| **Docker** | 15 min | Medium | ✅ Full (100%) |

**Recommendation:** Upgrade Python to 3.10+ for best experience.

---

## 🔍 Troubleshooting

### Error: "python3.10: command not found"

**Fix:**
```bash
# Find where Python 3.10 is installed
which python3.10

# If not found, install it
sudo apt install python3.10
```

### Error: "No module named 'ensurepip'"

**Fix:**
```bash
# Install pip for Python 3.10
sudo apt install python3.10-venv python3.10-dev
```

### Error: "permission denied"

**Fix:**
```bash
# Don't use sudo with pip in venv
# Just activate venv first:
source venv/bin/activate
pip install -r requirements.txt
```

### Still getting "Requires-Python >=3.10"?

Check which Python your venv is using:

```bash
# Activate venv
source venv/bin/activate

# Check Python version
python --version

# Check where Python is
which python

# Should be: /path/to/strands_etl/venv/bin/python
```

If it's still wrong:

```bash
# Delete and recreate venv with correct Python
deactivate
rm -rf venv
python3.10 -m venv venv
source venv/bin/activate
python --version  # Should now show 3.10+
```

---

## ✅ Verify Installation

After fixing Python version and installing requirements:

```bash
# Test Strands SDK
python -c "import strands; print('✓ Strands SDK:', strands.__version__)"

# Test boto3
python -c "import boto3; print('✓ boto3:', boto3.__version__)"

# Test pandas
python -c "import pandas; print('✓ pandas:', pandas.__version__)"

# Test all imports
python -c "
from strands_agents.orchestrator.config_loader import ConfigLoader
from strands_agents.tools.catalog_tools import detect_glue_table_size
print('✓ All imports successful!')
"
```

**Expected output:**
```
✓ Strands SDK: 1.23.0
✓ boto3: 1.26.0
✓ pandas: 2.0.0
✓ All imports successful!
```

---

## 🚀 Next Steps After Fixing

Once Python 3.10+ is installed and requirements are installed:

```bash
# Continue with Step 8 from MANUAL_SETUP_END_TO_END.md
cat MANUAL_SETUP_END_TO_END.md

# Or run examples
python example_config_driven_usage.py
```

---

## Quick Command Reference

```bash
# Check Python version
python3 --version

# Install Python 3.10 (Ubuntu/Debian)
sudo apt install python3.10 python3.10-venv

# Create venv with Python 3.10
python3.10 -m venv venv

# Activate venv
source venv/bin/activate

# Verify Python in venv
python --version

# Install requirements
pip install --upgrade pip
pip install -r requirements.txt

# Test installation
python -c "import strands; print(strands.__version__)"
```

---

**Need more help?** Check the error message carefully - it will tell you which specific package requires Python 3.10+.

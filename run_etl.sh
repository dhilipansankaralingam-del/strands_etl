#!/bin/bash
#
# ETL Framework - End-to-End Execution Shell Wrapper
# ===================================================
#
# Usage:
#   ./run_etl.sh --config demo_configs/complex_demo_config.json
#   ./run_etl.sh --config demo_configs/simple_demo_config.json --dry-run
#   ./run_etl.sh --simple                    # Run simple demo
#   ./run_etl.sh --complex                   # Run complex demo
#   ./run_etl.sh --complex --dry-run         # Dry run complex demo
#
# Environment Variables (optional):
#   SLACK_BOT_TOKEN      - Slack bot token for notifications
#   TEAMS_WEBHOOK_URL    - Teams webhook URL for notifications
#   EMAIL_SENDER         - SES verified sender email
#   AWS_REGION           - AWS region (default: us-east-1)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
CONFIG_FILE=""
DRY_RUN=""
VERBOSE=""

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║           ETL FRAMEWORK - END-TO-END EXECUTION                    ║"
    echo "║                                                                   ║"
    echo "║  Agents: Code Analysis | Workload | DQ | Compliance | Learning   ║"
    echo "║  Integrations: Slack | Teams | Email | CloudWatch                 ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --config, -c FILE    Path to configuration JSON file"
    echo "  --simple             Use simple demo config (demo_configs/simple_demo_config.json)"
    echo "  --complex            Use complex demo config (demo_configs/complex_demo_config.json)"
    echo "  --dry-run, -d        Run without executing actual ETL job"
    echo "  --verbose, -v        Verbose output"
    echo "  --help, -h           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --simple --dry-run"
    echo "  $0 --complex"
    echo "  $0 --config my_config.json"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config|-c)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --simple)
            CONFIG_FILE="demo_configs/simple_demo_config.json"
            shift
            ;;
        --complex)
            CONFIG_FILE="demo_configs/complex_demo_config.json"
            shift
            ;;
        --dry-run|-d)
            DRY_RUN="--dry-run"
            shift
            ;;
        --verbose|-v)
            VERBOSE="--verbose"
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

# Validate config file
if [ -z "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: No configuration file specified${NC}"
    echo ""
    usage
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Configuration file not found: $CONFIG_FILE${NC}"
    exit 1
fi

# Print banner
print_banner

# Print configuration
echo -e "${YELLOW}Configuration:${NC}"
echo "  Config File: $CONFIG_FILE"
echo "  Dry Run: ${DRY_RUN:-No}"
echo "  Verbose: ${VERBOSE:-No}"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is required but not installed${NC}"
    exit 1
fi

# Set Python path
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH}"

# Check if required modules exist
echo -e "${YELLOW}Checking framework modules...${NC}"
MODULES=(
    "framework/agents/auto_healing_agent.py"
    "framework/agents/code_analysis_agent.py"
    "framework/agents/compliance_agent.py"
    "framework/agents/data_quality_agent.py"
    "framework/agents/workload_assessment_agent.py"
    "framework/agents/learning_agent.py"
    "framework/agents/recommendation_agent.py"
)

ALL_PRESENT=true
for module in "${MODULES[@]}"; do
    if [ -f "$module" ]; then
        echo -e "  ${GREEN}✓${NC} $module"
    else
        echo -e "  ${RED}✗${NC} $module (missing)"
        ALL_PRESENT=false
    fi
done

if [ "$ALL_PRESENT" = false ]; then
    echo -e "${RED}Error: Some framework modules are missing${NC}"
    exit 1
fi

echo ""

# Check integrations (optional)
echo -e "${YELLOW}Checking integrations (optional)...${NC}"
if [ -n "$SLACK_BOT_TOKEN" ]; then
    echo -e "  ${GREEN}✓${NC} Slack: Configured"
else
    echo -e "  ${YELLOW}○${NC} Slack: Not configured (set SLACK_BOT_TOKEN)"
fi

if [ -n "$TEAMS_WEBHOOK_URL" ]; then
    echo -e "  ${GREEN}✓${NC} Teams: Configured"
else
    echo -e "  ${YELLOW}○${NC} Teams: Not configured (set TEAMS_WEBHOOK_URL)"
fi

if [ -n "$EMAIL_SENDER" ]; then
    echo -e "  ${GREEN}✓${NC} Email: Configured"
else
    echo -e "  ${YELLOW}○${NC} Email: Not configured (set EMAIL_SENDER)"
fi

echo ""

# Extract job name from config
JOB_NAME=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['job_name'])" 2>/dev/null || echo "unknown")
echo -e "${YELLOW}Job Name: ${NC}$JOB_NAME"
echo ""

# Confirm execution
if [ -z "$DRY_RUN" ]; then
    echo -e "${YELLOW}This will execute the ETL job. Continue? [y/N]${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Create output directory
mkdir -p output

# Run the ETL orchestrator
echo ""
echo -e "${GREEN}Starting ETL Execution...${NC}"
echo "═══════════════════════════════════════════════════════════════════════"

START_TIME=$(date +%s)

python3 scripts/run_etl.py --config "$CONFIG_FILE" $DRY_RUN $VERBOSE
EXIT_CODE=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ETL Execution Completed Successfully!${NC}"
    echo -e "${GREEN}  Total Time: ${DURATION} seconds${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════${NC}"
else
    echo -e "${RED}═══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}  ETL Execution Failed!${NC}"
    echo -e "${RED}  Exit Code: ${EXIT_CODE}${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════════════════════${NC}"
fi

# List generated reports
if [ -d "output" ] && [ "$(ls -A output 2>/dev/null)" ]; then
    echo ""
    echo -e "${YELLOW}Generated Reports:${NC}"
    ls -la output/*.html 2>/dev/null || true
fi

exit $EXIT_CODE

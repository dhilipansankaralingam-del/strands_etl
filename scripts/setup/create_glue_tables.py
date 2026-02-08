#!/usr/bin/env python3
"""
AWS Glue Catalog Setup
======================

Creates Glue databases and tables for the Sales Analytics ETL.

Databases:
- raw_data: Raw transactional data (orders, order_lines)
- master_data: Reference/dimension data (customers, products, regions)
- analytics: Transformed analytics tables (sales_fact, aggregates)

Usage:
    python scripts/setup/create_glue_tables.py --region us-east-1
    python scripts/setup/create_glue_tables.py --dry-run
"""

import os
import sys
import json
import argparse
import boto3
from typing import Dict, Any, List


# ============================================================================
# Table Definitions
# ============================================================================

S3_BUCKET = "etl-framework-data"

DATABASES = {
    "raw_data": "Raw transactional data for ETL processing",
    "master_data": "Master/reference data for joins and lookups",
    "analytics": "Transformed analytics and aggregated data"
}

TABLES = {
    # -------------------------------------------------------------------------
    # RAW DATA TABLES
    # -------------------------------------------------------------------------
    "raw_data.orders": {
        "description": "Raw order transactions",
        "location": f"s3://{S3_BUCKET}/raw/orders/",
        "format": "parquet",
        "partition_keys": [
            {"Name": "order_date", "Type": "string"}
        ],
        "columns": [
            {"Name": "order_id", "Type": "string", "Comment": "Unique order identifier"},
            {"Name": "customer_id", "Type": "string", "Comment": "Customer identifier"},
            {"Name": "customer_name", "Type": "string", "Comment": "Customer full name"},
            {"Name": "region_id", "Type": "string", "Comment": "Region identifier"},
            {"Name": "order_year", "Type": "int", "Comment": "Order year"},
            {"Name": "order_month", "Type": "int", "Comment": "Order month"},
            {"Name": "order_day", "Type": "int", "Comment": "Order day"},
            {"Name": "order_dayofweek", "Type": "int", "Comment": "Day of week (0=Monday)"},
            {"Name": "status", "Type": "string", "Comment": "Order status"},
            {"Name": "num_items", "Type": "int", "Comment": "Number of line items"},
            {"Name": "subtotal", "Type": "double", "Comment": "Order subtotal"},
            {"Name": "tax_rate", "Type": "double", "Comment": "Applied tax rate"},
            {"Name": "tax_amount", "Type": "double", "Comment": "Tax amount"},
            {"Name": "shipping_amount", "Type": "double", "Comment": "Shipping cost"},
            {"Name": "net_amount", "Type": "double", "Comment": "Total amount with tax and shipping"},
            {"Name": "total_cost", "Type": "double", "Comment": "Total cost of goods"},
            {"Name": "profit", "Type": "double", "Comment": "Order profit"},
            {"Name": "profit_margin", "Type": "double", "Comment": "Profit margin percentage"},
            {"Name": "payment_method", "Type": "string", "Comment": "Payment method used"},
            {"Name": "shipping_method", "Type": "string", "Comment": "Shipping method"},
            {"Name": "created_at", "Type": "timestamp", "Comment": "Record creation timestamp"},
            {"Name": "updated_at", "Type": "timestamp", "Comment": "Record update timestamp"}
        ]
    },

    "raw_data.order_lines": {
        "description": "Order line item details",
        "location": f"s3://{S3_BUCKET}/raw/order_lines/",
        "format": "parquet",
        "partition_keys": [
            {"Name": "order_date", "Type": "string"}
        ],
        "columns": [
            {"Name": "order_id", "Type": "string", "Comment": "Order identifier"},
            {"Name": "line_number", "Type": "int", "Comment": "Line item number"},
            {"Name": "product_id", "Type": "string", "Comment": "Product identifier"},
            {"Name": "product_name", "Type": "string", "Comment": "Product name"},
            {"Name": "category", "Type": "string", "Comment": "Product category"},
            {"Name": "quantity", "Type": "int", "Comment": "Quantity ordered"},
            {"Name": "unit_price", "Type": "double", "Comment": "Unit price"},
            {"Name": "unit_cost", "Type": "double", "Comment": "Unit cost"},
            {"Name": "discount", "Type": "double", "Comment": "Discount rate (0-1)"},
            {"Name": "discount_amount", "Type": "double", "Comment": "Discount amount"},
            {"Name": "line_total", "Type": "double", "Comment": "Line total after discount"},
            {"Name": "line_cost", "Type": "double", "Comment": "Line cost"},
            {"Name": "line_profit", "Type": "double", "Comment": "Line profit"}
        ]
    },

    # -------------------------------------------------------------------------
    # MASTER DATA TABLES
    # -------------------------------------------------------------------------
    "master_data.customers": {
        "description": "Customer master data",
        "location": f"s3://{S3_BUCKET}/master/customers/",
        "format": "parquet",
        "partition_keys": [],
        "columns": [
            {"Name": "customer_id", "Type": "string", "Comment": "Unique customer identifier"},
            {"Name": "customer_name", "Type": "string", "Comment": "Full name (PII)"},
            {"Name": "first_name", "Type": "string", "Comment": "First name (PII)"},
            {"Name": "last_name", "Type": "string", "Comment": "Last name (PII)"},
            {"Name": "customer_email", "Type": "string", "Comment": "Email address (PII)"},
            {"Name": "phone", "Type": "string", "Comment": "Phone number (PII)"},
            {"Name": "address", "Type": "string", "Comment": "Street address (PII)"},
            {"Name": "city", "Type": "string", "Comment": "City"},
            {"Name": "state", "Type": "string", "Comment": "State code"},
            {"Name": "zip_code", "Type": "string", "Comment": "ZIP code"},
            {"Name": "country", "Type": "string", "Comment": "Country code"},
            {"Name": "region_id", "Type": "string", "Comment": "Region identifier"},
            {"Name": "customer_tier", "Type": "string", "Comment": "Customer tier (Bronze/Silver/Gold/Platinum)"},
            {"Name": "credit_card_masked", "Type": "string", "Comment": "Masked credit card (PCI)"},
            {"Name": "created_date", "Type": "string", "Comment": "Account creation date"},
            {"Name": "updated_date", "Type": "string", "Comment": "Last update date"},
            {"Name": "is_active", "Type": "boolean", "Comment": "Active status"}
        ]
    },

    "master_data.products": {
        "description": "Product catalog",
        "location": f"s3://{S3_BUCKET}/master/products/",
        "format": "parquet",
        "partition_keys": [],
        "columns": [
            {"Name": "product_id", "Type": "string", "Comment": "Unique product identifier"},
            {"Name": "product_name", "Type": "string", "Comment": "Product name"},
            {"Name": "category", "Type": "string", "Comment": "Product category"},
            {"Name": "subcategory", "Type": "string", "Comment": "Product subcategory"},
            {"Name": "unit_price", "Type": "double", "Comment": "Standard unit price"},
            {"Name": "unit_cost", "Type": "double", "Comment": "Unit cost"},
            {"Name": "profit_margin", "Type": "double", "Comment": "Standard profit margin %"},
            {"Name": "weight_kg", "Type": "double", "Comment": "Product weight in kg"},
            {"Name": "supplier_id", "Type": "string", "Comment": "Supplier identifier"},
            {"Name": "is_active", "Type": "boolean", "Comment": "Active status"},
            {"Name": "created_date", "Type": "string", "Comment": "Product creation date"}
        ]
    },

    "master_data.regions": {
        "description": "Geographic regions",
        "location": f"s3://{S3_BUCKET}/master/regions/",
        "format": "parquet",
        "partition_keys": [],
        "columns": [
            {"Name": "region_id", "Type": "string", "Comment": "Region identifier"},
            {"Name": "region_name", "Type": "string", "Comment": "Region name"},
            {"Name": "country", "Type": "string", "Comment": "Country code"},
            {"Name": "tax_rate", "Type": "double", "Comment": "Regional tax rate"},
            {"Name": "shipping_zone", "Type": "int", "Comment": "Shipping zone (1-5)"},
            {"Name": "is_active", "Type": "boolean", "Comment": "Active status"}
        ]
    },

    # -------------------------------------------------------------------------
    # ANALYTICS TABLES
    # -------------------------------------------------------------------------
    # NOTE: analytics.sales_fact is created by the PySpark job using Iceberg
    # Do NOT pre-create Iceberg tables in Glue - Spark creates them automatically

    "analytics.customer_aggregates": {
        "description": "Customer-level aggregated metrics",
        "location": f"s3://{S3_BUCKET}/analytics/customer_aggregates/",
        "format": "parquet",
        "partition_keys": [
            {"Name": "snapshot_date", "Type": "string"}
        ],
        "columns": [
            {"Name": "customer_id", "Type": "string", "Comment": "Customer identifier"},
            {"Name": "customer_tier", "Type": "string", "Comment": "Customer tier"},
            {"Name": "region_id", "Type": "string", "Comment": "Region"},
            {"Name": "total_orders", "Type": "bigint", "Comment": "Total order count"},
            {"Name": "total_items", "Type": "bigint", "Comment": "Total items purchased"},
            {"Name": "total_revenue", "Type": "double", "Comment": "Total revenue"},
            {"Name": "total_profit", "Type": "double", "Comment": "Total profit"},
            {"Name": "avg_order_value", "Type": "double", "Comment": "Average order value"},
            {"Name": "avg_profit_margin", "Type": "double", "Comment": "Average profit margin"},
            {"Name": "first_order_date", "Type": "date", "Comment": "First order date"},
            {"Name": "last_order_date", "Type": "date", "Comment": "Most recent order date"},
            {"Name": "days_since_last_order", "Type": "int", "Comment": "Days since last order"},
            {"Name": "favorite_category", "Type": "string", "Comment": "Most purchased category"}
        ]
    },

    "analytics.product_performance": {
        "description": "Product performance metrics by period",
        "location": f"s3://{S3_BUCKET}/analytics/product_performance/",
        "format": "parquet",
        "partition_keys": [
            {"Name": "order_year", "Type": "int"},
            {"Name": "order_month", "Type": "int"}
        ],
        "columns": [
            {"Name": "product_id", "Type": "string", "Comment": "Product identifier"},
            {"Name": "product_name", "Type": "string", "Comment": "Product name"},
            {"Name": "category", "Type": "string", "Comment": "Category"},
            {"Name": "units_sold", "Type": "bigint", "Comment": "Total units sold"},
            {"Name": "order_count", "Type": "bigint", "Comment": "Number of orders"},
            {"Name": "total_revenue", "Type": "double", "Comment": "Total revenue"},
            {"Name": "total_cost", "Type": "double", "Comment": "Total cost"},
            {"Name": "total_profit", "Type": "double", "Comment": "Total profit"},
            {"Name": "avg_selling_price", "Type": "double", "Comment": "Average selling price"},
            {"Name": "avg_discount", "Type": "double", "Comment": "Average discount rate"},
            {"Name": "profit_margin", "Type": "double", "Comment": "Profit margin %"},
            {"Name": "unique_customers", "Type": "bigint", "Comment": "Unique customer count"}
        ]
    }
}


# ============================================================================
# Glue Catalog Manager
# ============================================================================

class GlueCatalogManager:
    """Manages AWS Glue Catalog resources."""

    def __init__(self, region: str = 'us-east-1', dry_run: bool = False):
        self.region = region
        self.dry_run = dry_run
        self.glue_client = None

        if not dry_run:
            self.glue_client = boto3.client('glue', region_name=region)

    def create_database(self, name: str, description: str) -> bool:
        """Create a Glue database."""
        print(f"\nCreating database: {name}")

        if self.dry_run:
            print(f"  [DRY RUN] Would create database: {name}")
            return True

        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': name,
                    'Description': description,
                    'Parameters': {
                        'created_by': 'etl_framework',
                        'purpose': 'sales_analytics'
                    }
                }
            )
            print(f"  ✓ Created database: {name}")
            return True

        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"  ⚠ Database already exists: {name}")
            return True

        except Exception as e:
            print(f"  ✗ Failed to create database: {e}")
            return False

    def create_table(self, database: str, table_name: str, table_def: Dict) -> bool:
        """Create a Glue table."""
        full_name = f"{database}.{table_name}"
        print(f"\nCreating table: {full_name}")

        # Build table input
        table_input = {
            'Name': table_name,
            'Description': table_def.get('description', ''),
            'StorageDescriptor': {
                'Columns': table_def['columns'],
                'Location': table_def['location'],
                'InputFormat': self._get_input_format(table_def['format']),
                'OutputFormat': self._get_output_format(table_def['format']),
                'Compressed': True,
                'SerdeInfo': self._get_serde_info(table_def['format']),
                'Parameters': {
                    'classification': table_def['format']
                }
            },
            'PartitionKeys': table_def.get('partition_keys', []),
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': table_def['format'],
                'created_by': 'etl_framework'
            }
        }

        # Add Delta-specific parameters
        if table_def['format'] == 'delta':
            table_input['Parameters']['spark.sql.sources.provider'] = 'delta'
            table_input['Parameters']['delta.lastCommitTimestamp'] = '0'

        # NOTE: Iceberg tables should be created by Spark, not Glue catalog API
        # The PySpark job will create the Iceberg table with proper metadata

        if self.dry_run:
            print(f"  [DRY RUN] Would create table: {full_name}")
            print(f"    Location: {table_def['location']}")
            print(f"    Format: {table_def['format']}")
            print(f"    Columns: {len(table_def['columns'])}")
            print(f"    Partitions: {[p['Name'] for p in table_def.get('partition_keys', [])]}")
            return True

        try:
            self.glue_client.create_table(
                DatabaseName=database,
                TableInput=table_input
            )
            print(f"  ✓ Created table: {full_name}")
            return True

        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"  ⚠ Table already exists: {full_name}")
            # Optionally update
            try:
                self.glue_client.update_table(
                    DatabaseName=database,
                    TableInput=table_input
                )
                print(f"  ✓ Updated existing table: {full_name}")
            except Exception as e:
                print(f"  ⚠ Could not update table: {e}")
            return True

        except Exception as e:
            print(f"  ✗ Failed to create table: {e}")
            return False

    def _get_input_format(self, format_type: str) -> str:
        """Get Hadoop input format for file type."""
        formats = {
            'parquet': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'delta': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'json': 'org.apache.hadoop.mapred.TextInputFormat',
            'csv': 'org.apache.hadoop.mapred.TextInputFormat'
        }
        return formats.get(format_type, formats['parquet'])

    def _get_output_format(self, format_type: str) -> str:
        """Get Hadoop output format for file type."""
        formats = {
            'parquet': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'delta': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'json': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'csv': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        }
        return formats.get(format_type, formats['parquet'])

    def _get_serde_info(self, format_type: str) -> Dict:
        """Get SerDe configuration for file type."""
        serdes = {
            'parquet': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'delta': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'json': {
                'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe',
                'Parameters': {}
            },
            'csv': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {'field.delim': ',', 'serialization.format': ','}
            }
        }
        return serdes.get(format_type, serdes['parquet'])

    def add_partitions(self, database: str, table_name: str, partitions: List[Dict]) -> bool:
        """Add partitions to a table."""
        if self.dry_run:
            print(f"  [DRY RUN] Would add {len(partitions)} partitions to {database}.{table_name}")
            return True

        try:
            # Batch add partitions (max 100 per call)
            for i in range(0, len(partitions), 100):
                batch = partitions[i:i+100]
                self.glue_client.batch_create_partition(
                    DatabaseName=database,
                    TableName=table_name,
                    PartitionInputList=batch
                )
            print(f"  ✓ Added {len(partitions)} partitions")
            return True
        except Exception as e:
            print(f"  ✗ Failed to add partitions: {e}")
            return False

    def run_crawler(self, crawler_name: str) -> bool:
        """Run a Glue crawler to discover partitions."""
        if self.dry_run:
            print(f"  [DRY RUN] Would run crawler: {crawler_name}")
            return True

        try:
            self.glue_client.start_crawler(Name=crawler_name)
            print(f"  ✓ Started crawler: {crawler_name}")
            return True
        except Exception as e:
            print(f"  ✗ Failed to start crawler: {e}")
            return False


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Create Glue Catalog tables for Sales Analytics ETL')
    parser.add_argument('--region', '-r', type=str, default='us-east-1',
                        help='AWS region (default: us-east-1)')
    parser.add_argument('--bucket', '-b', type=str, default='etl-framework-data',
                        help='S3 bucket for data (default: etl-framework-data)')
    parser.add_argument('--dry-run', '-d', action='store_true',
                        help='Show what would be created without actually creating')

    args = parser.parse_args()

    # Update bucket in table definitions
    global S3_BUCKET
    S3_BUCKET = args.bucket

    print("=" * 70)
    print("AWS GLUE CATALOG SETUP - Sales Analytics ETL")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Region:   {args.region}")
    print(f"  Bucket:   {args.bucket}")
    print(f"  Dry Run:  {args.dry_run}")

    # Initialize manager
    manager = GlueCatalogManager(region=args.region, dry_run=args.dry_run)

    # Create databases
    print("\n" + "-" * 70)
    print("Creating Databases")
    print("-" * 70)

    for db_name, description in DATABASES.items():
        manager.create_database(db_name, description)

    # Create tables
    print("\n" + "-" * 70)
    print("Creating Tables")
    print("-" * 70)

    for full_table_name, table_def in TABLES.items():
        # Update location with actual bucket
        table_def['location'] = table_def['location'].replace('etl-framework-data', args.bucket)

        database, table_name = full_table_name.split('.')
        manager.create_table(database, table_name, table_def)

    print("\n" + "=" * 70)
    print("Glue Catalog setup complete!")
    print("=" * 70)

    # Print summary
    print("\nCreated Resources:")
    print(f"  Databases: {len(DATABASES)}")
    print(f"  Tables:    {len(TABLES)}")

    print("\nNext Steps:")
    print("-" * 70)
    print("1. Run a Glue Crawler to discover partitions:")
    print(f"   aws glue start-crawler --name etl-data-crawler --region {args.region}")
    print("\n2. Or manually add partitions using MSCK REPAIR:")
    print("   MSCK REPAIR TABLE raw_data.orders;")
    print("   MSCK REPAIR TABLE raw_data.order_lines;")
    print("\n3. Verify tables in Athena:")
    print("   SELECT * FROM raw_data.orders LIMIT 10;")


if __name__ == "__main__":
    main()

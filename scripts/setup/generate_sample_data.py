#!/usr/bin/env python3
"""
Sample Data Generator for Complex Sales Analytics ETL
======================================================

Generates realistic sample data for:
- orders (transactions)
- order_lines (line items)
- customers (master data)
- products (master data)
- regions (master data)

Usage:
    python scripts/setup/generate_sample_data.py --records 100000 --output ./sample_data
    python scripts/setup/generate_sample_data.py --records 1000000 --output s3://bucket/data --upload
"""

import os
import sys
import json
import random
import string
import argparse
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import csv

# Try to import optional dependencies
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    print("[WARN] pyarrow not installed. Will generate CSV files instead of Parquet.")

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# ============================================================================
# Data Generation Configuration
# ============================================================================

# Customer configuration
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "email.org"]

STREET_TYPES = ["Street", "Avenue", "Boulevard", "Lane", "Drive", "Court", "Way", "Road"]

CITIES = [
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"), ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"), ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"), ("Dallas", "TX", "75201"),
    ("San Jose", "CA", "95101"), ("Austin", "TX", "73301"), ("Jacksonville", "FL", "32201"),
    ("Fort Worth", "TX", "76101"), ("Columbus", "OH", "43201"), ("Charlotte", "NC", "28201"),
    ("Seattle", "WA", "98101"), ("Denver", "CO", "80201"), ("Boston", "MA", "02101"),
    ("Portland", "OR", "97201"), ("Atlanta", "GA", "30301"), ("Miami", "FL", "33101")
]

# Product configuration
PRODUCT_CATEGORIES = {
    "Electronics": [
        ("Laptop", 799.99, 1299.99), ("Smartphone", 399.99, 999.99), ("Tablet", 299.99, 799.99),
        ("Headphones", 49.99, 349.99), ("Smart Watch", 199.99, 499.99), ("Camera", 299.99, 1499.99),
        ("Monitor", 149.99, 799.99), ("Keyboard", 29.99, 199.99), ("Mouse", 19.99, 129.99),
        ("Speaker", 49.99, 399.99), ("Charger", 14.99, 79.99), ("Cable", 9.99, 49.99)
    ],
    "Clothing": [
        ("T-Shirt", 14.99, 49.99), ("Jeans", 29.99, 99.99), ("Jacket", 49.99, 199.99),
        ("Shoes", 39.99, 149.99), ("Dress", 29.99, 149.99), ("Sweater", 24.99, 89.99),
        ("Shorts", 19.99, 59.99), ("Socks", 4.99, 19.99), ("Hat", 9.99, 39.99),
        ("Scarf", 14.99, 49.99), ("Belt", 19.99, 79.99), ("Gloves", 9.99, 49.99)
    ],
    "Home & Garden": [
        ("Furniture", 99.99, 999.99), ("Lamp", 24.99, 149.99), ("Rug", 49.99, 299.99),
        ("Curtains", 29.99, 99.99), ("Pillow", 14.99, 79.99), ("Blanket", 24.99, 149.99),
        ("Vase", 14.99, 79.99), ("Clock", 19.99, 99.99), ("Mirror", 29.99, 199.99),
        ("Plant Pot", 9.99, 49.99), ("Garden Tool", 14.99, 99.99), ("Outdoor Chair", 49.99, 249.99)
    ],
    "Sports": [
        ("Running Shoes", 59.99, 199.99), ("Yoga Mat", 19.99, 79.99), ("Dumbbells", 24.99, 149.99),
        ("Basketball", 19.99, 49.99), ("Tennis Racket", 49.99, 199.99), ("Golf Club", 99.99, 499.99),
        ("Bicycle", 199.99, 999.99), ("Helmet", 29.99, 149.99), ("Sports Bag", 24.99, 99.99),
        ("Water Bottle", 9.99, 39.99), ("Fitness Tracker", 49.99, 249.99), ("Resistance Bands", 14.99, 49.99)
    ],
    "Books": [
        ("Novel", 9.99, 29.99), ("Textbook", 29.99, 149.99), ("Cookbook", 14.99, 49.99),
        ("Biography", 12.99, 34.99), ("Self-Help", 9.99, 29.99), ("Science Fiction", 9.99, 24.99),
        ("History", 14.99, 39.99), ("Art Book", 24.99, 99.99), ("Travel Guide", 12.99, 34.99),
        ("Children's Book", 7.99, 24.99), ("Magazine", 4.99, 14.99), ("Comic Book", 4.99, 19.99)
    ]
}

# Region configuration
REGIONS = [
    ("NORTHEAST", "Northeast", "US"),
    ("SOUTHEAST", "Southeast", "US"),
    ("MIDWEST", "Midwest", "US"),
    ("SOUTHWEST", "Southwest", "US"),
    ("WEST", "West", "US"),
    ("PACIFIC", "Pacific", "US"),
    ("CANADA_EAST", "Eastern Canada", "CA"),
    ("CANADA_WEST", "Western Canada", "CA"),
    ("EUROPE_WEST", "Western Europe", "EU"),
    ("EUROPE_EAST", "Eastern Europe", "EU")
]

# State to region mapping
STATE_REGION_MAP = {
    "NY": "NORTHEAST", "MA": "NORTHEAST", "PA": "NORTHEAST", "NJ": "NORTHEAST",
    "FL": "SOUTHEAST", "GA": "SOUTHEAST", "NC": "SOUTHEAST", "VA": "SOUTHEAST",
    "IL": "MIDWEST", "OH": "MIDWEST", "MI": "MIDWEST", "IN": "MIDWEST",
    "TX": "SOUTHWEST", "AZ": "SOUTHWEST", "NM": "SOUTHWEST", "OK": "SOUTHWEST",
    "CA": "WEST", "CO": "WEST", "WA": "PACIFIC", "OR": "PACIFIC"
}


# ============================================================================
# Data Generator Classes
# ============================================================================

class DataGenerator:
    """Main data generator class."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.customer_id_counter = 0
        self.product_id_counter = 0
        self.order_id_counter = 0

    def generate_customers(self, count: int) -> List[Dict]:
        """Generate customer records."""
        customers = []

        for i in range(count):
            self.customer_id_counter += 1
            customer_id = f"CUST{self.customer_id_counter:08d}"

            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(EMAIL_DOMAINS)}"

            city, state, zip_code = random.choice(CITIES)
            address = f"{random.randint(100, 9999)} {random.choice(LAST_NAMES)} {random.choice(STREET_TYPES)}"

            # Generate masked phone and credit card for compliance testing
            phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
            credit_card = f"{''.join([str(random.randint(0,9)) for _ in range(16)])}"
            credit_card_masked = f"****-****-****-{credit_card[-4:]}"

            # Customer tier based on random distribution
            tier = random.choices(
                ["Bronze", "Silver", "Gold", "Platinum"],
                weights=[0.5, 0.3, 0.15, 0.05]
            )[0]

            created_date = datetime.now() - timedelta(days=random.randint(30, 1825))

            customers.append({
                "customer_id": customer_id,
                "customer_name": f"{first_name} {last_name}",
                "first_name": first_name,
                "last_name": last_name,
                "customer_email": email,
                "phone": phone,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "country": "US",
                "region_id": STATE_REGION_MAP.get(state, "WEST"),
                "customer_tier": tier,
                "credit_card_masked": credit_card_masked,
                "created_date": created_date.strftime("%Y-%m-%d"),
                "updated_date": datetime.now().strftime("%Y-%m-%d"),
                "is_active": random.random() > 0.1
            })

        return customers

    def generate_products(self, count_per_category: int = 20) -> List[Dict]:
        """Generate product records."""
        products = []

        for category, items in PRODUCT_CATEGORIES.items():
            for item_name, min_price, max_price in items:
                # Generate variations
                for i in range(count_per_category // len(items) + 1):
                    self.product_id_counter += 1
                    product_id = f"PROD{self.product_id_counter:06d}"

                    variation = f" - Variant {i+1}" if i > 0 else ""
                    base_price = round(random.uniform(min_price, max_price), 2)
                    cost = round(base_price * random.uniform(0.3, 0.6), 2)

                    products.append({
                        "product_id": product_id,
                        "product_name": f"{item_name}{variation}",
                        "category": category,
                        "subcategory": item_name,
                        "unit_price": base_price,
                        "unit_cost": cost,
                        "profit_margin": round((base_price - cost) / base_price * 100, 2),
                        "weight_kg": round(random.uniform(0.1, 20.0), 2),
                        "supplier_id": f"SUP{random.randint(1, 50):03d}",
                        "is_active": random.random() > 0.05,
                        "created_date": (datetime.now() - timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d")
                    })

                    if len(products) >= count_per_category * len(PRODUCT_CATEGORIES):
                        break

        return products[:count_per_category * len(PRODUCT_CATEGORIES)]

    def generate_regions(self) -> List[Dict]:
        """Generate region records."""
        regions = []
        for region_id, region_name, country in REGIONS:
            regions.append({
                "region_id": region_id,
                "region_name": region_name,
                "country": country,
                "tax_rate": round(random.uniform(0.05, 0.12), 4),
                "shipping_zone": random.randint(1, 5),
                "is_active": True
            })
        return regions

    def generate_orders_and_lines(
        self,
        order_count: int,
        customers: List[Dict],
        products: List[Dict],
        start_date: datetime = None,
        end_date: datetime = None
    ) -> Tuple[List[Dict], List[Dict]]:
        """Generate orders and order_lines."""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()

        orders = []
        order_lines = []

        customer_ids = [c["customer_id"] for c in customers]
        product_list = products

        date_range = (end_date - start_date).days

        for _ in range(order_count):
            self.order_id_counter += 1
            order_id = f"ORD{self.order_id_counter:010d}"

            # Random order date
            order_date = start_date + timedelta(days=random.randint(0, date_range))

            # Random customer
            customer_id = random.choice(customer_ids)
            customer = next(c for c in customers if c["customer_id"] == customer_id)

            # Order status based on date
            days_ago = (datetime.now() - order_date).days
            if days_ago < 3:
                status = random.choices(
                    ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED"],
                    weights=[0.3, 0.3, 0.3, 0.1]
                )[0]
            elif days_ago < 14:
                status = random.choices(
                    ["PROCESSING", "SHIPPED", "DELIVERED"],
                    weights=[0.1, 0.4, 0.5]
                )[0]
            else:
                status = random.choices(
                    ["SHIPPED", "DELIVERED", "RETURNED", "CANCELLED"],
                    weights=[0.05, 0.85, 0.05, 0.05]
                )[0]

            # Generate line items (1-8 per order)
            num_lines = random.choices([1, 2, 3, 4, 5, 6, 7, 8], weights=[0.2, 0.3, 0.2, 0.15, 0.08, 0.04, 0.02, 0.01])[0]

            order_products = random.sample(product_list, min(num_lines, len(product_list)))

            order_total = 0
            order_cost = 0
            line_num = 0

            for product in order_products:
                line_num += 1
                quantity = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.25, 0.15, 0.07, 0.03])[0]
                unit_price = product["unit_price"]
                unit_cost = product["unit_cost"]

                # Random discount (0-30%)
                discount = round(random.choices(
                    [0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3],
                    weights=[0.4, 0.2, 0.15, 0.1, 0.08, 0.05, 0.02]
                )[0], 2)

                line_total = round(quantity * unit_price * (1 - discount), 2)
                line_cost = round(quantity * unit_cost, 2)
                line_profit = round(line_total - line_cost, 2)

                order_lines.append({
                    "order_id": order_id,
                    "line_number": line_num,
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "category": product["category"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "unit_cost": unit_cost,
                    "discount": discount,
                    "discount_amount": round(quantity * unit_price * discount, 2),
                    "line_total": line_total,
                    "line_cost": line_cost,
                    "line_profit": line_profit,
                    "order_date": order_date.strftime("%Y-%m-%d")
                })

                order_total += line_total
                order_cost += line_cost

            # Calculate order-level metrics
            tax_rate = 0.08  # 8% tax
            tax_amount = round(order_total * tax_rate, 2)
            shipping = round(random.uniform(5.99, 25.99), 2) if order_total < 100 else 0
            net_amount = round(order_total + tax_amount + shipping, 2)
            profit = round(order_total - order_cost, 2)

            orders.append({
                "order_id": order_id,
                "customer_id": customer_id,
                "customer_name": customer["customer_name"],
                "region_id": customer["region_id"],
                "order_date": order_date.strftime("%Y-%m-%d"),
                "order_year": order_date.year,
                "order_month": order_date.month,
                "order_day": order_date.day,
                "order_dayofweek": order_date.weekday(),
                "status": status,
                "num_items": num_lines,
                "subtotal": round(order_total, 2),
                "tax_rate": tax_rate,
                "tax_amount": tax_amount,
                "shipping_amount": shipping,
                "net_amount": net_amount,
                "total_cost": round(order_cost, 2),
                "profit": profit,
                "profit_margin": round(profit / order_total * 100, 2) if order_total > 0 else 0,
                "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]),
                "shipping_method": random.choice(["STANDARD", "EXPRESS", "OVERNIGHT", "PICKUP"]),
                "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        return orders, order_lines


# ============================================================================
# File Writers
# ============================================================================

def write_csv(data: List[Dict], filepath: str):
    """Write data to CSV file."""
    if not data:
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"  Written: {filepath} ({len(data)} records)")


def write_parquet(data: List[Dict], filepath: str):
    """Write data to Parquet file."""
    if not HAS_PYARROW:
        # Fallback to CSV
        csv_path = filepath.replace('.parquet', '.csv')
        write_csv(data, csv_path)
        return

    if not data:
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Convert to PyArrow table
    table = pa.Table.from_pylist(data)
    pq.write_table(table, filepath, compression='snappy')

    print(f"  Written: {filepath} ({len(data)} records)")


def write_partitioned_parquet(data: List[Dict], base_path: str, partition_col: str):
    """Write data partitioned by a column."""
    if not HAS_PYARROW:
        # Fallback to single CSV
        csv_path = os.path.join(base_path, "data.csv")
        write_csv(data, csv_path)
        return

    if not data:
        return

    os.makedirs(base_path, exist_ok=True)

    # Group by partition column
    partitions = {}
    for record in data:
        key = record.get(partition_col, "unknown")
        if key not in partitions:
            partitions[key] = []
        partitions[key].append(record)

    # Write each partition
    for partition_value, partition_data in partitions.items():
        partition_path = os.path.join(base_path, f"{partition_col}={partition_value}")
        os.makedirs(partition_path, exist_ok=True)

        table = pa.Table.from_pylist(partition_data)
        pq.write_table(
            table,
            os.path.join(partition_path, "data.parquet"),
            compression='snappy'
        )

    print(f"  Written: {base_path} ({len(data)} records in {len(partitions)} partitions)")


# ============================================================================
# S3 Upload
# ============================================================================

def upload_to_s3(local_path: str, s3_path: str):
    """Upload local directory to S3."""
    if not HAS_BOTO3:
        print("[ERROR] boto3 not installed. Cannot upload to S3.")
        return False

    # Parse S3 path
    if not s3_path.startswith("s3://"):
        print(f"[ERROR] Invalid S3 path: {s3_path}")
        return False

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    s3_client = boto3.client('s3')

    print(f"\nUploading to s3://{bucket}/{prefix}...")

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = os.path.join(prefix, relative_path).replace("\\", "/")

            try:
                s3_client.upload_file(local_file, bucket, s3_key)
                print(f"  Uploaded: s3://{bucket}/{s3_key}")
            except Exception as e:
                print(f"  [ERROR] Failed to upload {local_file}: {e}")
                return False

    return True


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate sample data for Sales Analytics ETL')
    parser.add_argument('--records', '-r', type=int, default=100000,
                        help='Number of order records to generate (default: 100000)')
    parser.add_argument('--customers', '-c', type=int, default=10000,
                        help='Number of customers (default: 10000)')
    parser.add_argument('--products', '-p', type=int, default=500,
                        help='Number of products (default: 500)')
    parser.add_argument('--output', '-o', type=str, default='./sample_data',
                        help='Output directory or S3 path')
    parser.add_argument('--upload', '-u', action='store_true',
                        help='Upload to S3 after generation')
    parser.add_argument('--s3-bucket', type=str, default='etl-framework-data',
                        help='S3 bucket for upload')
    parser.add_argument('--format', '-f', choices=['parquet', 'csv'], default='parquet',
                        help='Output format (default: parquet)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')

    args = parser.parse_args()

    print("=" * 70)
    print("SAMPLE DATA GENERATOR - Sales Analytics ETL")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Orders:    {args.records:,}")
    print(f"  Customers: {args.customers:,}")
    print(f"  Products:  {args.products:,}")
    print(f"  Output:    {args.output}")
    print(f"  Format:    {args.format}")

    # Initialize generator
    generator = DataGenerator(seed=args.seed)

    # Generate data
    print("\n" + "-" * 70)
    print("Generating data...")
    print("-" * 70)

    print("\n1. Generating regions...")
    regions = generator.generate_regions()
    print(f"   Generated {len(regions)} regions")

    print("\n2. Generating customers...")
    customers = generator.generate_customers(args.customers)
    print(f"   Generated {len(customers)} customers")

    print("\n3. Generating products...")
    products = generator.generate_products(args.products // len(PRODUCT_CATEGORIES))
    print(f"   Generated {len(products)} products")

    print("\n4. Generating orders and order lines...")
    orders, order_lines = generator.generate_orders_and_lines(
        args.records, customers, products
    )
    print(f"   Generated {len(orders)} orders with {len(order_lines)} line items")

    # Write data
    print("\n" + "-" * 70)
    print("Writing data files...")
    print("-" * 70)

    output_base = args.output if not args.output.startswith("s3://") else "./sample_data"
    ext = ".parquet" if args.format == "parquet" else ".csv"

    # Master data (non-partitioned)
    print("\nMaster data:")
    master_path = os.path.join(output_base, "master")

    if args.format == "parquet":
        write_parquet(regions, os.path.join(master_path, "regions", f"regions{ext}"))
        write_parquet(customers, os.path.join(master_path, "customers", f"customers{ext}"))
        write_parquet(products, os.path.join(master_path, "products", f"products{ext}"))
    else:
        write_csv(regions, os.path.join(master_path, "regions", f"regions{ext}"))
        write_csv(customers, os.path.join(master_path, "customers", f"customers{ext}"))
        write_csv(products, os.path.join(master_path, "products", f"products{ext}"))

    # Raw data (partitioned by order_date)
    print("\nRaw data (partitioned):")
    raw_path = os.path.join(output_base, "raw")

    if args.format == "parquet":
        write_partitioned_parquet(orders, os.path.join(raw_path, "orders"), "order_date")
        write_partitioned_parquet(order_lines, os.path.join(raw_path, "order_lines"), "order_date")
    else:
        write_csv(orders, os.path.join(raw_path, "orders", f"orders{ext}"))
        write_csv(order_lines, os.path.join(raw_path, "order_lines", f"order_lines{ext}"))

    # Generate summary
    print("\n" + "-" * 70)
    print("Data Summary")
    print("-" * 70)

    total_revenue = sum(o["net_amount"] for o in orders)
    total_profit = sum(o["profit"] for o in orders)
    avg_order_value = total_revenue / len(orders) if orders else 0

    print(f"\n  Total Orders:      {len(orders):,}")
    print(f"  Total Line Items:  {len(order_lines):,}")
    print(f"  Total Revenue:     ${total_revenue:,.2f}")
    print(f"  Total Profit:      ${total_profit:,.2f}")
    print(f"  Avg Order Value:   ${avg_order_value:.2f}")
    print(f"  Profit Margin:     {total_profit/total_revenue*100:.1f}%")

    # Upload to S3 if requested
    if args.upload:
        print("\n" + "-" * 70)
        print("Uploading to S3...")
        print("-" * 70)

        s3_base = f"s3://{args.s3_bucket}"

        # Upload master data
        upload_to_s3(
            os.path.join(output_base, "master", "regions"),
            f"{s3_base}/master/regions/"
        )
        upload_to_s3(
            os.path.join(output_base, "master", "customers"),
            f"{s3_base}/master/customers/"
        )
        upload_to_s3(
            os.path.join(output_base, "master", "products"),
            f"{s3_base}/master/products/"
        )

        # Upload raw data
        upload_to_s3(
            os.path.join(output_base, "raw", "orders"),
            f"{s3_base}/raw/orders/"
        )
        upload_to_s3(
            os.path.join(output_base, "raw", "order_lines"),
            f"{s3_base}/raw/order_lines/"
        )

    print("\n" + "=" * 70)
    print("Data generation complete!")
    print("=" * 70)

    # Print next steps
    print("\nNext Steps:")
    print("-" * 70)
    if not args.upload:
        print(f"1. Upload data to S3:")
        print(f"   aws s3 sync {output_base}/master s3://{args.s3_bucket}/master/")
        print(f"   aws s3 sync {output_base}/raw s3://{args.s3_bucket}/raw/")
    print(f"\n2. Create Glue catalog tables (see scripts/setup/create_glue_tables.py)")
    print(f"\n3. Run the ETL:")
    print(f"   ./run_etl.sh --complex")


if __name__ == "__main__":
    main()

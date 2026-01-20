"""
Example: Using Strands Quality Agent for SQL and Code Analysis

This example demonstrates how to use the Quality Agent independently
to analyze SQL queries, PySpark code, and natural language quality requests.
"""

import asyncio
import json
from strands.strands_message_bus import get_message_bus, start_message_bus, MessageType
from strands.strands_quality_agent import StrandsQualityAgent


async def example_sql_analysis():
    """Example: Analyze SQL query for performance issues."""
    print("\n=== SQL Query Analysis ===\n")

    # Start message bus
    bus_task = asyncio.create_task(start_message_bus())
    await asyncio.sleep(1)

    # Create and start Quality Agent
    agent = StrandsQualityAgent()
    agent_task = asyncio.create_task(agent.start())
    await asyncio.sleep(2)

    # Get message bus
    bus = get_message_bus()

    # Send SQL analysis request
    sql_query = """
    SELECT * FROM customers
    JOIN orders ON customers.customer_id = orders.customer_id
    JOIN products ON orders.product_id = products.product_id
    WHERE UPPER(customer_name) = 'JOHN'
    """

    quality_msg = bus.create_message(
        sender='example_app',
        message_type=MessageType.QUALITY_CHECK,
        payload={
            'check_type': 'sql',
            'input': sql_query
        },
        target='quality_agent'
    )

    await bus.publish(quality_msg)

    # Wait for response
    await asyncio.sleep(5)

    # Get message history
    messages = bus.get_message_history(correlation_id=quality_msg.correlation_id)

    for msg in messages:
        if msg.message_type == MessageType.AGENT_RESPONSE:
            result = msg.payload
            print(f"Quality Score: {result.get('query_analysis', {}).get('quality_score', 0):.2f}")
            print(f"\nIssues Found: {len(result.get('query_analysis', {}).get('issues', []))}")
            for issue in result.get('query_analysis', {}).get('issues', []):
                print(f"  - [{issue['severity'].upper()}] {issue['message']}")
                print(f"    Recommendation: {issue['recommendation']}")

            print(f"\nAI Analysis:")
            print(f"  {result.get('query_analysis', {}).get('ai_analysis', 'N/A')}")

    await agent.stop()


async def example_code_analysis():
    """Example: Analyze PySpark code for anti-patterns."""
    print("\n=== PySpark Code Analysis ===\n")

    bus_task = asyncio.create_task(start_message_bus())
    await asyncio.sleep(1)

    agent = StrandsQualityAgent()
    agent_task = asyncio.create_task(agent.start())
    await asyncio.sleep(2)

    bus = get_message_bus()

    # Sample PySpark code with anti-patterns
    pyspark_code = """
# Read data
df = spark.read.parquet("s3://bucket/data")

# Check count (triggers action)
print(f"Records: {df.count()}")

# Multiple joins without broadcast
result = df.join(customers, "customer_id", "inner")
result = result.join(products, "product_id", "left")
result = result.join(categories, "category_id", "left")

# Another count
print(f"After join: {result.count()}")

# Collect all data (dangerous!)
all_data = result.collect()

# Show results
result.show()
"""

    code_msg = bus.create_message(
        sender='example_app',
        message_type=MessageType.QUALITY_CHECK,
        payload={
            'check_type': 'code',
            'input': pyspark_code
        },
        target='quality_agent'
    )

    await bus.publish(code_msg)
    await asyncio.sleep(5)

    messages = bus.get_message_history(correlation_id=code_msg.correlation_id)

    for msg in messages:
        if msg.message_type == MessageType.AGENT_RESPONSE:
            result = msg.payload
            code_analysis = result.get('code_analysis', {})
            print(f"Code Quality Score: {code_analysis.get('quality_score', 0):.2f}")
            print(f"\nIssues Found: {len(code_analysis.get('issues', []))}")

            for issue in code_analysis.get('issues', []):
                print(f"  - Line {issue['line']}: [{issue['severity'].upper()}] {issue['message']}")
                print(f"    Code: {issue['code_snippet']}")
                print(f"    Fix: {issue['recommendation']}\n")

            print(f"AI Review:")
            print(f"  {code_analysis.get('ai_review', 'N/A')}")

    await agent.stop()


async def example_natural_language_quality():
    """Example: Natural language quality check request."""
    print("\n=== Natural Language Quality Check ===\n")

    bus_task = asyncio.create_task(start_message_bus())
    await asyncio.sleep(1)

    agent = StrandsQualityAgent()
    agent_task = asyncio.create_task(agent.start())
    await asyncio.sleep(2)

    bus = get_message_bus()

    # Natural language request
    nl_request = "Check if all customer records have valid email addresses and check for duplicate customer IDs"

    nl_msg = bus.create_message(
        sender='example_app',
        message_type=MessageType.QUALITY_CHECK,
        payload={
            'check_type': 'natural_language',
            'input': nl_request
        },
        target='quality_agent'
    )

    await bus.publish(nl_msg)
    await asyncio.sleep(8)  # NL processing takes longer

    messages = bus.get_message_history(correlation_id=nl_msg.correlation_id)

    for msg in messages:
        if msg.message_type == MessageType.AGENT_RESPONSE:
            result = msg.payload
            print(f"Natural Language Input: '{result.get('natural_language_input')}'")
            print(f"\nInterpreted Quality Checks:")

            for check in result.get('interpreted_checks', []):
                print(f"  - Type: {check['check_type']}")
                print(f"    Description: {check['description']}")
                print(f"    Target: {check['target']}")
                print(f"    Threshold: {check.get('threshold', 'N/A')}\n")

            print(f"Overall Quality Score: {result.get('overall_quality_score', 0):.2f}")

    await agent.stop()


async def main():
    """Run all examples."""
    print("=" * 60)
    print("Strands Quality Agent Examples")
    print("=" * 60)

    try:
        # Example 1: SQL Analysis
        await example_sql_analysis()

        # Wait between examples
        await asyncio.sleep(2)

        # Example 2: Code Analysis
        await example_code_analysis()

        # Wait between examples
        await asyncio.sleep(2)

        # Example 3: Natural Language
        await example_natural_language_quality()

    except KeyboardInterrupt:
        print("\nExamples interrupted by user")


if __name__ == '__main__':
    asyncio.run(main())

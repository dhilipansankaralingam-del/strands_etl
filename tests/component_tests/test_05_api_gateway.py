#!/usr/bin/env python3
"""
Component Test 5: API Gateway Integration
Tests API Gateway handlers and endpoints
"""

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from integrations.api_gateway import (
    APIGatewayIntegration,
    APIConfig,
    APIResponse,
    ETLAPIHandlers,
    create_api_handler,
    HTTPMethod,
    AuthType
)


def test_api_config_creation():
    """Test API config creation"""
    print("\n  Testing: API Config Creation")
    print("  " + "-" * 50)

    try:
        config = APIConfig(
            api_name="test-etl-api",
            stage="test",
            region="us-east-1",
            auth_type=AuthType.API_KEY,
            rate_limit=1000,
            enable_cors=True
        )

        print(f"    api_name: {config.api_name}")
        print(f"    stage: {config.stage}")
        print(f"    region: {config.region}")
        print(f"    auth_type: {config.auth_type.value}")
        print(f"    rate_limit: {config.rate_limit}")
        print(f"    enable_cors: {config.enable_cors}")

        print("    [PASS] API config created")
        return True

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_api_response():
    """Test API response structure"""
    print("\n  Testing: API Response Structure")
    print("  " + "-" * 50)

    try:
        response = APIResponse(
            status_code=200,
            body={"message": "Success", "data": {"count": 10}},
            headers={"X-Custom-Header": "test"}
        )

        lambda_response = response.to_lambda_response()

        assert lambda_response["statusCode"] == 200
        assert "body" in lambda_response
        assert "headers" in lambda_response
        assert lambda_response["headers"]["Content-Type"] == "application/json"
        assert lambda_response["headers"]["X-Custom-Header"] == "test"

        body = json.loads(lambda_response["body"])
        assert body["message"] == "Success"
        assert body["data"]["count"] == 10

        print(f"    statusCode: {lambda_response['statusCode']}")
        print(f"    Content-Type: {lambda_response['headers']['Content-Type']}")
        print(f"    CORS headers present: {'Access-Control-Allow-Origin' in lambda_response['headers']}")

        print("    [PASS] API response structure correct")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_route_registration():
    """Test route registration"""
    print("\n  Testing: Route Registration")
    print("  " + "-" * 50)

    try:
        config = APIConfig()
        api = APIGatewayIntegration(config)

        # Register some routes
        api.register_route("/test", HTTPMethod.GET, lambda **kwargs: APIResponse(200, {}))
        api.register_route("/test", HTTPMethod.POST, lambda **kwargs: APIResponse(201, {}))
        api.register_route("/test/{id}", HTTPMethod.GET, lambda **kwargs: APIResponse(200, {}))

        assert "/test" in api._routes
        assert "GET" in api._routes["/test"]
        assert "POST" in api._routes["/test"]
        assert "/test/{id}" in api._routes

        print(f"    Routes registered: {len(api._routes)}")
        for path, methods in api._routes.items():
            print(f"      {path}: {list(methods.keys())}")

        print("    [PASS] Routes registered correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_health_endpoint():
    """Test health check endpoint"""
    print("\n  Testing: Health Check Endpoint")
    print("  " + "-" * 50)

    try:
        api = create_api_handler(APIConfig())

        event = {
            'httpMethod': 'GET',
            'path': '/health',
            'headers': {},
            'queryStringParameters': {},
            'pathParameters': {},
            'body': None
        }

        response = api.handle_request(event, None)

        assert response['statusCode'] == 200

        body = json.loads(response['body'])
        assert body['status'] == 'healthy'
        assert 'timestamp' in body
        assert 'version' in body

        print(f"    Status Code: {response['statusCode']}")
        print(f"    Health Status: {body['status']}")
        print(f"    Version: {body['version']}")

        print("    [PASS] Health endpoint works")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_cors_preflight():
    """Test CORS preflight handling"""
    print("\n  Testing: CORS Preflight (OPTIONS)")
    print("  " + "-" * 50)

    try:
        api = create_api_handler(APIConfig())

        event = {
            'httpMethod': 'OPTIONS',
            'path': '/jobs',
            'headers': {},
            'queryStringParameters': {},
            'pathParameters': {},
            'body': None
        }

        response = api.handle_request(event, None)

        assert response['statusCode'] == 200
        assert 'Access-Control-Allow-Origin' in response['headers']
        assert 'Access-Control-Allow-Methods' in response['headers']

        print(f"    Status Code: {response['statusCode']}")
        print(f"    Allow-Origin: {response['headers']['Access-Control-Allow-Origin']}")
        print(f"    Allow-Methods: {response['headers']['Access-Control-Allow-Methods']}")

        print("    [PASS] CORS preflight handled correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_404_not_found():
    """Test 404 for unknown routes"""
    print("\n  Testing: 404 Not Found")
    print("  " + "-" * 50)

    try:
        api = create_api_handler(APIConfig())

        event = {
            'httpMethod': 'GET',
            'path': '/nonexistent/route',
            'headers': {},
            'queryStringParameters': {},
            'pathParameters': {},
            'body': None
        }

        response = api.handle_request(event, None)

        assert response['statusCode'] == 404

        body = json.loads(response['body'])
        assert 'error' in body

        print(f"    Status Code: {response['statusCode']}")
        print(f"    Error: {body['error']}")

        print("    [PASS] 404 handled correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_all_registered_routes():
    """Test all registered routes"""
    print("\n  Testing: All Registered Routes")
    print("  " + "-" * 50)

    try:
        api = create_api_handler(APIConfig())

        expected_routes = [
            ("/health", "GET"),
            ("/jobs", "GET"),
            ("/metrics", "GET"),
        ]

        print(f"    Total routes: {len(api._routes)}")
        for path, methods in api._routes.items():
            for method in methods.keys():
                print(f"      {method} {path}")

        for path, method in expected_routes:
            assert path in api._routes, f"Missing route: {path}"
            assert method in api._routes[path], f"Missing method {method} for {path}"

        print("    [PASS] All expected routes registered")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def run_tests():
    """Run all API Gateway tests"""
    print("=" * 60)
    print("COMPONENT TEST 5: API Gateway Integration")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    tests = [
        test_api_config_creation,
        test_api_response,
        test_route_registration,
        test_health_endpoint,
        test_cors_preflight,
        test_404_not_found,
        test_all_registered_routes
    ]

    for test_func in tests:
        if test_func():
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = results["passed"] + results["failed"]
    print(f"  Total:  {total}")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")

    if results["failed"] == 0:
        print("\n  [SUCCESS] All API Gateway tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

"""
Bronze Layer entrypoint (Backwards-compatible alias for bronze_layer.py).
"""
from apps.bronze_layer import run_bronze_pipeline

if __name__ == "__main__":
    run_bronze_pipeline()
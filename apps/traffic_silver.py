"""
Silver Layer entrypoint (Backwards-compatible alias for silver_layer.py).
"""
from apps.silver_layer import run_silver_pipeline

if __name__ == "__main__":
    run_silver_pipeline()
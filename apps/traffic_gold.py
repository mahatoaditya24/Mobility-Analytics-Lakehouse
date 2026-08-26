"""
Gold Layer entrypoint (Backwards-compatible alias for gold_layer.py).
"""
from apps.gold_layer import run_gold_pipeline

if __name__ == "__main__":
    run_gold_pipeline()
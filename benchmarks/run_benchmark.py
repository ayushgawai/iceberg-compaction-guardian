"""Run the benchmark and commit its output under benchmarks/."""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from guardian.benchmark import run_benchmark

if __name__ == "__main__":
    result = run_benchmark()
    out = Path(__file__).with_name("results.json")
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"\nwrote {out}")

from guardian.benchmark import run_benchmark

def test_benchmark_smoke():
    r = run_benchmark(rows=3000, files=12, target_bytes=64 * 1024, repeats=3)
    assert r["rows_preserved"]
    assert r["after"]["files"] <= r["before"]["files"]
    assert r["contract"]["pass_rate"] == 1.0

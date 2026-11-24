import json
from guardian.cli import main

def _run(capsys, argv):
    code = main(argv)
    return code, json.loads(capsys.readouterr().out)

def test_cli_end_to_end(tmp_path, capsys):
    table = str(tmp_path / "events")
    code, out = _run(capsys, ["--table", table, "ingest", "--rows", "500", "--files", "10"])
    assert code == 0 and out["live_files"] == 10
    code, out = _run(capsys, ["--table", table, "validate"])
    assert code == 0 and out["passed"] and out["row_count"] == 500
    code, out = _run(capsys, ["--table", table, "compact", "--target-bytes", "65536"])
    assert code == 0 and out["after_files"] < out["before_files"]
    code, out = _run(capsys, ["--table", table, "time-travel", "--list"])
    assert code == 0 and out["snapshots"][-1]["operation"] == "compact"
    code, out = _run(capsys, ["--table", table, "status"])
    assert code == 0 and out["rows"] == 500

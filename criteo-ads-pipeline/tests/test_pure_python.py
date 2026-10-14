"""不需要 Spark 的单元测试：python -m pytest tests 或 python tests/test_pure_python.py"""
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load(path, name):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _rowcount_check():
    # 只取出纯 Python 函数，避免导入 pyspark
    src = (ROOT / "jobs" / "silver.py").read_text()
    start = src.index("def rowcount_check")
    end = src.index("\ndef ", start + 1)
    ns = {}
    exec(src[start:end], ns)
    return ns["rowcount_check"]


def test_rowcount_check():
    f = _rowcount_check()
    daily = [(0, 100), (1, 110), (2, 90), (3, 100), (4, 10)]
    out = {r["day"]: r for r in f(daily, lookback=7, tolerance=0.3)}
    assert 0 not in out                       # 第 0 天没有历史
    assert out[1]["status"] == "PASS"         # 110 vs 100 → +10%
    assert out[4]["status"] == "WARN"         # 10 vs 100 → -90%
    assert out[4]["history_days"] == 4
    # lookback 只看前 2 天
    out2 = {r["day"]: r for r in f(daily, lookback=2, tolerance=0.3)}
    assert out2[3]["trailing_mean"] == 100.0  # (110+90)/2


def test_summarize():
    m = _load(ROOT / "experiments" / "summarize.py", "summarize")
    base = {c: "" for c in m.COLS}
    rows = [
        {**base, "experiment": "e", "variant": "a", "is_warmup": "True", "wall_s": "99"},
        {**base, "experiment": "e", "variant": "a", "is_warmup": "False", "wall_s": "1", "n_tasks": "4"},
        {**base, "experiment": "e", "variant": "a", "is_warmup": "False", "wall_s": "3", "n_tasks": "4"},
        {**base, "experiment": "e", "variant": "a", "is_warmup": "False", "wall_s": "2", "n_tasks": "4"},
    ]
    text = m.summarize(rows)
    assert "| a | 3 | 2.00 | 4 |" in text   # warmup 的 99 不参与中位数
    assert "99" not in text


if __name__ == "__main__":
    test_rowcount_check()
    test_summarize()
    print("all tests passed")

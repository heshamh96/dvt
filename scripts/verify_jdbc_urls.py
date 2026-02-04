#!/usr/bin/env python3
"""Verify all JDBC driver URLs from core/dvt/task/jdbc_drivers.ADAPTER_TO_JDBC_DRIVERS."""
import sys
from pathlib import Path

# Import the registry from the core package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "core"))
from dvt.task.jdbc_drivers import ADAPTER_TO_JDBC_DRIVERS

def maven_jar_url(group_id: str, artifact_id: str, version: str) -> str:
    path = group_id.replace(".", "/") + f"/{artifact_id}/{version}/{artifact_id}-{version}.jar"
    return "https://repo1.maven.org/maven2/" + path

def main():
    import urllib.request
    results = []
    for adapter, drivers in sorted(ADAPTER_TO_JDBC_DRIVERS.items()):
        for (g, a, v) in drivers:
            url = maven_jar_url(g, a, v)
            try:
                req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "DVT-Check/1.0"})
                with urllib.request.urlopen(req, timeout=15) as r:
                    status = r.status
            except Exception as e:
                status = str(e)
            results.append((adapter, g, a, v, url, status))
    for adapter, g, a, v, url, status in results:
        mark = "OK" if status == 200 else "FAIL"
        print(f"{mark} {adapter} {g}:{a}:{v}")
        if status != 200:
            print(f"    URL: {url}")
            print(f"    Status: {status}")
    print()
    ok_count = sum(1 for r in results if r[5] == 200)
    print(f"Result: {ok_count}/{len(results)} URLs OK")
    return 0 if all(r[5] == 200 for r in results) else 1

if __name__ == "__main__":
    sys.exit(main())

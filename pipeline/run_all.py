import json
import time
from pathlib import Path

from pipeline.common import (
    DELTA_WRITE_METRICS,
    RUN_STARTED_AT,
    RUN_TIMESTAMP,
    cleanup_spark_local_dir,
    infer_stage,
    load_config,
    spark_session,
)
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.raw_profile import raw_profile_mode, run_raw_profile
from pipeline.stream_ingest import run_stream_ingestion
from pipeline.transform import run_transformation


def _read_int(path):
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
        if value and value != "max":
            return int(value)
    except (FileNotFoundError, OSError, ValueError):
        return None
    return None


def _read_cpu_stat():
    try:
        lines = Path("/sys/fs/cgroup/cpu.stat").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, OSError):
        return {}
    stats = {}
    for line in lines:
        parts = line.split()
        if len(parts) == 2:
            try:
                stats[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return stats


def _cpu_quota_cores():
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
        if quota != "max":
            return round(int(quota) / int(period), 2)
    except (FileNotFoundError, OSError, ValueError):
        return None
    return None


def _resource_snapshot():
    return {
        "memory_current_bytes": _read_int("/sys/fs/cgroup/memory.current"),
        "memory_peak_bytes": _read_int("/sys/fs/cgroup/memory.peak"),
        "memory_limit_bytes": _read_int("/sys/fs/cgroup/memory.max"),
        "cpu_quota_cores": _cpu_quota_cores(),
        "cpu_stat": _read_cpu_stat(),
    }


def _resource_delta(start, end):
    start_cpu = (start or {}).get("cpu_stat") or {}
    end_cpu = (end or {}).get("cpu_stat") or {}
    usage_usec = end_cpu.get("usage_usec")
    if usage_usec is None:
        return {}
    start_usage_usec = start_cpu.get("usage_usec", 0)
    return {
        "cpu_usage_seconds": round((usage_usec - start_usage_usec) / 1_000_000, 3),
        "cpu_user_seconds": round((end_cpu.get("user_usec", 0) - start_cpu.get("user_usec", 0)) / 1_000_000, 3),
        "cpu_system_seconds": round((end_cpu.get("system_usec", 0) - start_cpu.get("system_usec", 0)) / 1_000_000, 3),
        "nr_throttled": end_cpu.get("nr_throttled", 0) - start_cpu.get("nr_throttled", 0),
        "throttled_seconds": round((end_cpu.get("throttled_usec", 0) - start_cpu.get("throttled_usec", 0)) / 1_000_000, 3),
    }


def _performance_path(config):
    configured = (config.get("performance") or {}).get("profile_path")
    if configured:
        return configured
    output_root = Path(config["output"].get("gold_path", "/data/output/gold")).parent
    return str(output_root / "audit" / "performance_profile.json")


def _write_performance_profile(config, profile):
    profile["total_wall_seconds"] = round(time.time() - RUN_STARTED_AT, 3)
    profile["delta_writes"] = DELTA_WRITE_METRICS
    profile["resource_end"] = _resource_snapshot()
    profile["resource_delta"] = _resource_delta(profile.get("resource_start"), profile["resource_end"])
    quota = profile["resource_end"].get("cpu_quota_cores")
    cpu_seconds = profile["resource_delta"].get("cpu_usage_seconds")
    if quota and cpu_seconds is not None and profile["total_wall_seconds"] > 0:
        profile["avg_cpu_utilisation_pct_of_quota"] = round(cpu_seconds * 100.0 / (profile["total_wall_seconds"] * quota), 2)
    path = Path(_performance_path(config))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2, sort_keys=False)


def _time_phase(profile, name, func, allow_failure=False):
    phase = {
        "name": name,
        "started_at_second": round(time.time() - RUN_STARTED_AT, 3),
        "status": "running",
    }
    profile["phases"].append(phase)
    started_at = time.time()
    try:
        result = func()
        phase["status"] = "ok"
        return result
    except Exception as exc:
        phase["status"] = "skipped" if allow_failure else "failed"
        phase["error"] = str(exc)
        if allow_failure:
            print(f"{name} skipped: {exc}")
            return None
        raise
    finally:
        phase["duration_seconds"] = round(time.time() - started_at, 3)
        phase["ended_at_second"] = round(time.time() - RUN_STARTED_AT, 3)


def main():
    config = load_config()
    profile = {
        "$schema": "nedbank-de-challenge/performance-profile/v1",
        "run_timestamp": RUN_TIMESTAMP,
        "phases": [],
        "resource_start": _resource_snapshot(),
    }
    spark = _time_phase(profile, "spark_session_init", lambda: spark_session(config))
    profile_mode = raw_profile_mode(config)
    try:
        _time_phase(profile, "ingest", run_ingestion)
        if profile_mode == "full":
            _time_phase(profile, "raw_profile_full", run_raw_profile, allow_failure=True)
        _time_phase(profile, "transform", run_transformation)
        if profile_mode == "light":
            _time_phase(profile, "raw_profile_light", run_raw_profile, allow_failure=True)
        _time_phase(profile, "provision", run_provisioning)
        if infer_stage(config) == "3":
            _time_phase(profile, "stream_ingest", run_stream_ingestion)
    finally:
        _time_phase(profile, "spark_stop", spark.stop)
        _time_phase(profile, "cleanup_spark_local_dir", lambda: cleanup_spark_local_dir(config))
        _write_performance_profile(config, profile)


if __name__ == "__main__":
    main()

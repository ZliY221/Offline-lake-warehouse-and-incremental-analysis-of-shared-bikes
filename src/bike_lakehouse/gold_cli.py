"""Command-line Gold analytics build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .gold import build_gold_analytics
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gold daily metrics and popular routes")
    parser.add_argument("--silver-trips", type=Path, required=True)
    parser.add_argument("--station-dimension", type=Path, required=True)
    parser.add_argument("--daily-metrics", type=Path, required=True)
    parser.add_argument("--popular-routes", type=Path, required=True)
    parser.add_argument("--route-limit", type=int, default=3)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-gold", Path("build/spark-warehouse"))
    try:
        result = build_gold_analytics(
            spark,
            silver_trip_path=args.silver_trips,
            station_dimension_path=args.station_dimension,
            daily_metrics_path=args.daily_metrics,
            popular_routes_path=args.popular_routes,
            route_limit=args.route_limit,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

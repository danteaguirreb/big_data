"""CLI academica para ejecutar la Fase 2."""

from __future__ import annotations

import argparse
import json

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Fase 2 Gestion Territorial El Monte")
    parser.add_argument("--scale-mode", default="small", choices=["small", "medium", "full"])
    parser.add_argument("--root", default=".")
    parser.add_argument("--tmp-root", default=None)
    parser.add_argument("--warehouse-dir", default=None)
    parser.add_argument("--checkpoint-dir", default=None)
    parser.add_argument("--driver-memory", default=None)
    args = parser.parse_args()
    summary = run_pipeline(
        scale_mode=args.scale_mode,
        root=args.root,
        tmp_root=args.tmp_root,
        warehouse_dir=args.warehouse_dir,
        checkpoint_dir=args.checkpoint_dir,
        driver_memory=args.driver_memory,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

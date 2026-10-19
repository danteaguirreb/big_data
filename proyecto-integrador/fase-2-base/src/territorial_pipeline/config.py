"""Configuracion reproducible para perfiles de escala."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    solicitudes: int
    eventos: int
    logs: int
    fotos_factor: float
    satisfaccion_factor: float
    shuffle_partitions: int


PROFILES = {
    "small": ScaleProfile(
        name="small",
        solicitudes=5_000,
        eventos=120_000,
        logs=35_000,
        fotos_factor=0.70,
        satisfaccion_factor=0.45,
        shuffle_partitions=8,
    ),
    "medium": ScaleProfile(
        name="medium",
        solicitudes=50_000,
        eventos=2_000_000,
        logs=500_000,
        fotos_factor=0.75,
        satisfaccion_factor=0.50,
        shuffle_partitions=24,
    ),
    "full": ScaleProfile(
        name="full",
        solicitudes=50_000,
        eventos=10_800_000,
        logs=1_500_000,
        fotos_factor=0.80,
        satisfaccion_factor=0.55,
        shuffle_partitions=96,
    ),
}


def get_profile(scale_mode: str) -> ScaleProfile:
    key = scale_mode.lower().strip()
    if key not in PROFILES:
        allowed = ", ".join(PROFILES)
        raise ValueError(f"SCALE_MODE invalido: {scale_mode}. Valores permitidos: {allowed}")
    return PROFILES[key]


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    data: Path
    bronze: Path
    silver: Path
    gold: Path
    evidence: Path


def build_paths(root: str | Path = ".") -> PipelinePaths:
    base = Path(root).resolve()
    data = base / "data"
    return PipelinePaths(
        root=base,
        data=data,
        bronze=data / "bronze",
        silver=data / "silver",
        gold=data / "gold",
        evidence=base / "evidence",
    )

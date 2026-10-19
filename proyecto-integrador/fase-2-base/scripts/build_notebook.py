"""Genera el notebook academico autocontenido de la Fase 2."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import nbformat as nbf


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "fase2_pipeline_gestion_territorial.ipynb"


def md(text: str):
    return nbf.v4.new_markdown_cell(dedent(text).strip())


def code(text: str):
    return nbf.v4.new_code_cell(dedent(text).strip())


nb = nbf.v4.new_notebook()
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "pygments_lexer": "ipython3"},
}

nb["cells"] = [
    md(
        """
        # Fase 2 - Pipeline Big Data Gestion Territorial El Monte

        Notebook ejecutable para demostrar una arquitectura lakehouse medallion con PySpark, datos sinteticos a escala, evidencia de optimizacion y lineamientos de gobierno/seguridad.
        """
    ),
    md(
        """
        ## 1. Introduccion y objetivo

        La Fase 2 implementa en la practica la arquitectura definida para la Municipalidad de El Monte. El foco no es construir una plataforma productiva completa, sino un pipeline reproducible que cubra ingesta, transformacion, almacenamiento Parquet, KPIs, optimizacion y documentacion tecnica.
        """
    ),
    md(
        """
        ## 2. Instalacion/importacion de librerias

        En Google Colab se puede descomentar la instalacion. Localmente basta con instalar `requirements.txt`.
        """
    ),
    code(
        """
        # !pip install -q pyspark==3.5.1

        import json
        import os
        import sys
        from pathlib import Path

        PROJECT_ROOT = Path.cwd()
        if not (PROJECT_ROOT / "src").exists() and (PROJECT_ROOT.parent / "src").exists():
            PROJECT_ROOT = PROJECT_ROOT.parent
        if (PROJECT_ROOT / "src").exists():
            sys.path.insert(0, str(PROJECT_ROOT / "src"))

        from territorial_pipeline.config import get_profile
        from territorial_pipeline.pipeline import run_pipeline
        """
    ),
    md("## 3. Configuracion Spark"),
    code(
        """
        SCALE_MODE = os.environ.get("SCALE_MODE", "small")
        profile = get_profile(SCALE_MODE)

        print(f"SCALE_MODE: {profile.name}")
        print(f"Solicitudes: {profile.solicitudes:,}")
        print(f"Eventos: {profile.eventos:,}")
        print(f"Logs: {profile.logs:,}")
        print(f"Shuffle partitions: {profile.shuffle_partitions}")
        """
    ),
    md(
        """
        ## 4. Parametros globales

        - `small`: validacion local rapida.
        - `medium`: 50.000 solicitudes y 2 millones de eventos.
        - `full`: 50.000 solicitudes y mas de 10 millones de eventos, recomendado para cluster Spark.
        """
    ),
    code(
        """
        expected_bronze_rows = (
            profile.solicitudes
            + profile.eventos
            + profile.logs
            + int(profile.solicitudes * profile.fotos_factor)
            + int(profile.solicitudes * profile.satisfaccion_factor)
            + 2922 * 4
            + 32
        )
        print(f"Filas Bronze esperadas aproximadamente: {expected_bronze_rows:,}")
        """
    ),
    md(
        """
        ## 5. Generacion de datos sinteticos

        La generacion usa `spark.range` y expresiones Spark para escalar sin crear listas grandes en memoria. Las fuentes incluyen solicitudes, eventos, logs, metadatos de fotografias, clima, cuadrillas y satisfaccion.
        """
    ),
    md(
        """
        ## 6. Ingesta Bronze

        La ejecucion escribe las fuentes crudas en Parquet bajo `data/bronze`, manteniendo estructura cercana al origen y registrando conteos por tabla.
        """
    ),
    md(
        """
        ## 7. Transformaciones Silver

        Silver elimina duplicados, valida nulos criticos, fechas y coordenadas, calcula tiempos entre eventos y enriquece solicitudes con clima, cuadrillas, fotografias, SLA y flags operacionales.
        """
    ),
    md(
        """
        ## 8. Agregaciones Gold

        Gold publica `kpi_sla`, `backlog_operacional`, `zonas_criticas`, `satisfaccion_ciudadana`, `desempeno_cuadrillas` y `dataset_modelo_priorizacion`.
        """
    ),
    md(
        """
        ## 9. Optimizacion base vs optimizada

        El experimento compara Parquet sin particionamiento contra Parquet particionado por `anio`, `mes`, `zona_gestion` y `tipo_evento`, usando un filtro selectivo existente para evidenciar pruning.
        """
    ),
    code(
        """
        summary = run_pipeline(scale_mode=SCALE_MODE, root=PROJECT_ROOT)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        """
    ),
    md("## 10. Metricas de rendimiento"),
    code(
        """
        run_summary_path = PROJECT_ROOT / "evidence" / "run_summary.json"
        opt_path = PROJECT_ROOT / "evidence" / "optimization_metrics.json"

        run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))
        optimization = json.loads(opt_path.read_text(encoding="utf-8"))

        print("Resumen de ejecucion:")
        print(json.dumps(run_summary, indent=2, ensure_ascii=False))
        print("\\nExperimentos de optimizacion:")
        print(json.dumps(optimization, indent=2, ensure_ascii=False))
        """
    ),
    md("## 11. KPIs para dashboard"),
    code(
        """
        gold_path = PROJECT_ROOT / "data" / "gold"
        print("Datasets Gold exportados:")
        for path in sorted(gold_path.iterdir()):
            if path.is_dir():
                print("-", path.name)
        """
    ),
    md(
        """
        ## 12. Dataset para modelo de priorizacion

        El dataset evita data leakage: usa variables disponibles al ingreso, contexto climatico y variables simuladas historicas/territoriales. El target `urgencia_operativa` es sintetico y sirve como base para una futura prueba de modelo supervisado.
        """
    ),
    code(
        """
        print("Dataset modelo:", run_summary["gold_counts"].get("dataset_modelo_priorizacion"), "filas")
        """
    ),
    md(
        """
        ## 13. Gobierno, seguridad y linaje

        La documentacion asociada queda en:

        - `docs/gobierno_seguridad.md`
        - `docs/linaje_datos.md`
        - `docs/finops_estimacion.md`
        - `docs/dashboard_mockup.md`
        """
    ),
    code(
        """
        for doc in ["gobierno_seguridad.md", "linaje_datos.md", "finops_estimacion.md", "dashboard_mockup.md"]:
            path = PROJECT_ROOT / "docs" / doc
            print(f"{doc}: {'OK' if path.exists() else 'FALTA'}")
        """
    ),
    md(
        """
        ## 14. Conclusiones tecnicas

        El pipeline demuestra una arquitectura medallion reproducible, con datos sinteticos a escala, separacion Bronze/Silver/Gold, Parquet, KPIs operacionales y un benchmark de particionamiento. En modo `full`, Bronze supera los 10 millones de filas y justifica el uso de Spark.
        """
    ),
    md(
        """
        ## 15. Proximos pasos

        - Ejecutar `full` en un cluster Spark administrado.
        - Publicar Gold en BigQuery.
        - Crear dashboard Looker Studio.
        - Agregar Cloud Logging/Monitoring.
        - Formalizar IAM, KMS y politicas de retencion.
        - Entrenar y validar un modelo auxiliar de priorizacion con revision humana.
        """
    ),
]


NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, NOTEBOOK_PATH)
print(NOTEBOOK_PATH)

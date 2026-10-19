.PHONY: run-small run-medium run-full test

run-small:
	PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode small --root .

run-medium:
	PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode medium --root .

run-full:
	PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode full --root .

test:
	PYTHONPATH=src pytest -q

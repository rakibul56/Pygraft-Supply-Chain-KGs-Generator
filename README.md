# Pygraft Supply Chain Generator

This project generates a synthetic supply-chain knowledge graph from a schema
and a small set of configuration files. The main entrypoint is a CLI that builds
the schema artifacts, generates instance data, and writes an RDF graph to disk.

## Requirements

- Python 3.8+
- Java (only if you enable consistency checking with HermiT)

## Install

PowerShell (Windows):

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Run

After installing, the `pygraft-sc` command is available:

```powershell
python -m pygraft.cli.pygraft_sc --format ttl --out supply_chain_kg.ttl

full example with optional flags:

python -m cli.pygraft_sc --skip-consistency --format ttl --schema pygraft/domains/supply_chain/ontology/schema_standard.yaml --seed 43 --out graph.ttl
```

Optional flags:

- `--schema` points to a schema file (see `pygraft/domains/supply_chain/ontology/`)
- `--seed` fixes randomness for repeatable output
- `--check-consistency` runs HermiT (requires Java and can be slow)

## Output

Two outputs are written:

- The CLI `--out` file (e.g., `supply_chain_kg.ttl`) in the current folder
- The generated graph in `output/<schema_name>/full_graph.<ext>`

The extension matches the chosen format (`ttl`, `json-ld`, `nt`, `rdf`, or `xml`).

## Configuration

Main configuration lives in:

- `pygraft/domains/supply_chain/config/generation.yaml`
- `pygraft/domains/supply_chain/config/value_profiles.yaml`

Common parameters in `generation.yaml`:

- `num_entities`, `num_triples`: size of the generated graph
- `relation_balance_ratio`: how balanced relations are
- `prop_untyped_entities`: fraction of untyped entities
- `avg_depth_specific_class`: expected depth of specific class assignments
- `multityping`: allow entities to have multiple specific classes
- `avg_multityping`: average number of specific classes per entity (required when `multityping: true`)
- `fast_gen`, `oversample`: speed/quality tradeoffs
- `relation_distribution`, `popularity_skew`: control skew in relation and class usage
- `dataproperty_priority`: priority for which data properties to populate
- `kg_check_reasoner`, `reasoner_java_memory_mb`: consistency checking settings

`value_profiles.yaml` controls how literal values are generated for data
properties (faker sources, codes, date ranges, distributions, and choices).

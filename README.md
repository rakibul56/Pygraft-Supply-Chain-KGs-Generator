<div align="center">

# Artificial Generation of Graph Data Using Real-World Configurations

### Synthetic Supply Chain Knowledge Graph Generator

*A configuration-driven, ontology-aware extension of [PyGraft](https://github.com/nicolas-hbt/pygraft) for generating realistic, OWL-consistent supply-chain knowledge graphs.*

Master's thesis — TU Chemnitz.

[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![RDF / OWL](https://img.shields.io/badge/Knowledge%20Graph-RDF%20%7C%20OWL%202-0A7E8C)](https://www.w3.org/TR/owl2-overview/)
[![Reasoner](https://img.shields.io/badge/Reasoner-HermiT-4B8BBE)](http://www.hermit-reasoner.com/)
[![Domain](https://img.shields.io/badge/Domain-Supply%20Chain-2E8B57)](#schema-variants)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](#license)

[Features](#features) · [Pipeline](#end-to-end-pipeline) · [Quickstart](#quickstart) · [Configuration](#configuration) · [Examples](#example-output)

</div>

---

## Overview

Upstream **PyGraft** generates generic synthetic RDF graphs with placeholder names (`C1`, `E1`, `R1`). **PyGraft-SC** replaces those placeholders with a real, configurable **supply-chain domain** — farms, mills, harvest batches, shipments, logistics providers, retailers — driven entirely by YAML.

The generator produces:

- a reasoned **OWL TBox** (`schema.rdf`) from a YAML ontology
- an **ABox** of instance data that satisfies OWL axioms *and* supply-chain business rules
- realistic datatype literals (dates, codes, weights, names) with **temporal dependencies** between properties
- a final KG in Turtle / RDF-XML / N-Triples / JSON-LD, optionally verified with the **HermiT** reasoner

Everything is reproducible via a single `--seed` flag.

---

## Features

- **Three progressive ontologies** — `minimal`, `standard`, `extended` — covering class hierarchy, disjointness, inverse / functional / asymmetric / irreflexive properties, and property hierarchies.
- **Three-layer consistency enforcement**
  1. OWL TBox validation (HermiT on `schema.rdf`)
  2. Per-triple checks during generation (functional / inverse-functional / asymmetric / irreflexive conflicts)
  3. Post-generation supply-chain rules — chain-of-custody, role separation, single-owner-per-facility, non-circular shipments, grouped shipment routes, orphan-facility pruning
- **Value Profile Engine** with 13 literal generators (`truncated_normal`, `uniform`, `date_range`, `relation_offset`, `property_offset`, `code`, weighted `choice`, `faker`, …) and priority-ordered temporal dependencies.
- **Tunable statistics** — Pareto / Zipf / uniform popularity skews, per-relation hotspots, configurable multi-typing depth, balanced-vs-imbalanced relation distributions.
- **Domain-specific Faker providers** for readable entity labels.
- **Deterministic** — identical `--seed` ⇒ identical graph.

---

## End-to-End Pipeline

The generator runs in two stages sharing a single handoff contract — JSON checkpoints written by the schema stage and consumed by the instance stage.

**Stage 1 — Schema (TBox)**

1. Parse and validate `schema_*.yaml` (classes, relations, dataproperties).
2. Build an OWL ontology and serialize it to `schema.rdf`.
3. Write JSON checkpoints: `class_info.json`, `relation_info.json`, `dataproperty_info.json`.
4. *(Optional)* Run HermiT to confirm the ontology is consistent.

**Stage 2 — Instances (ABox)**

1. Load the JSON checkpoints plus `generation.yaml` and `value_profiles.yaml`.
2. Create typed entities and sample object-property triples under OWL consistency checks.
3. Apply supply-chain business rules (chain-of-custody, role separation, shipment routing, …).
4. Generate realistic literal values via the Value Profile Engine.
5. Serialize the final KG to `full_graph.<ttl | nt | rdf | jsonld>`.
6. *(Optional)* Run HermiT on the complete graph.

---

## Quickstart

### Requirements

- **Python** 3.8+
- **Java** 8+ *(only required when HermiT consistency checking is enabled)*

### Install

```bash
git clone https://github.com/<your-username>/pygraft-sc.git
cd pygraft-sc

python -m venv .venv
# Windows
.\.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

The project is installed in editable mode, so changes under `pygraft/` take effect immediately.

### Run

```bash
# Default: standard schema, Turtle output, HermiT on
python -m cli.pygraft_sc --format ttl --out supply_chain_kg.ttl

# Fast demo (skip reasoner)
python -m cli.pygraft_sc \
    --schema pygraft/domains/supply_chain/ontology/schema_minimal.yaml \
    --skip-consistency --format ttl --seed 42 --out demo_minimal.ttl

# Full extended schema
python -m cli.pygraft_sc \
    --schema pygraft/domains/supply_chain/ontology/schema_extended.yaml \
    --format ttl --seed 42 --out demo_extended.ttl
```

### CLI Options

| Flag | Default | Purpose |
|---|---|---|
| `--schema` | `schema_standard.yaml` | Path to a schema YAML file |
| `--format` | `ttl` | Output serialization: `ttl`, `json-ld`, `nt`, `rdf`, `xml` |
| `--seed` | `42` | Deterministic random seed |
| `--out` | `supply_chain_kg.ttl` | Destination file for the final graph |
| `--check-consistency` / `--skip-consistency` | `check` | Toggle HermiT reasoning |

---

## Schema Variants

| Variant | Classes | Object Properties | Datatype Properties | File |
|---|---:|---:|---:|---|
| **Minimal**  | 4  | 5  | 3  | `pygraft/domains/supply_chain/ontology/schema_minimal.yaml`  |
| **Standard** | 11 | 12 | 8  | `pygraft/domains/supply_chain/ontology/schema_standard.yaml` |
| **Extended** | 25 | 40 | 15 | `pygraft/domains/supply_chain/ontology/schema_extended.yaml` |

Each variant is a fully self-contained ontology — pick based on the complexity you want to showcase or benchmark.

---

## Example Output

Statistics from the committed artifacts under `output/` (generated with the default configuration and `--seed 42`).

| Schema | Entities | Instantiated Relations | Triples |
|---|---:|---:|---:|
| `supply_chain_minimal`  | 3,664 | 5  | 4,455  |
| `supply_chain`          | 2,271 | 11 | 2,424  |
| `supply_chain_extended` | 5,493 | 40 | 14,491 |

A typical Turtle snippet from `full_graph.ttl`:

```turtle
sc:Entity_42  a  sc:HarvestBatch ;
    rdfs:label       "Batch Oakridge-2024-07" ;
    sc:harvestedFrom sc:Entity_17 ;
    sc:harvestDate   "2024-07-14"^^xsd:date ;
    sc:harvestWeightKg "1342.5"^^xsd:decimal ;
    sc:qualityGrade  "A" .

sc:Entity_91  a  sc:Shipment ;
    sc:carriesBatch  sc:Entity_42 ;
    sc:shippedFrom   sc:Entity_17 ;
    sc:shippedTo     sc:Entity_58 ;
    sc:shippedBy     sc:Entity_33 ;
    sc:shipmentDate  "2024-07-21"^^xsd:date ;
    sc:shipmentStatus "in_transit" .
```

---

## Generated Artifacts

Each run writes to two locations:

- the path given by `--out` (current working directory)
- a structured directory at `output/<schema_name>/`

```
output/supply_chain/
├── schema.rdf                 # OWL TBox
├── class_info.json            # Parsed class hierarchy + disjointness
├── relation_info.json         # Object properties + characteristics
├── dataproperty_info.json     # Datatype properties
├── kg_info.json               # Run metrics (counts, avg depth, multi-typing)
└── full_graph.<ext>           # Final TBox + ABox
```

---

## Configuration

### 1. Ontology — `ontology/schema_*.yaml`

```yaml
classes:
  HarvestBatch:
    parents: [MaterialLot]
    disjoint_with: [Shipment]

relations:
  harvestedFrom:
    domain: HarvestBatch
    range:  Farm
    characteristics: [owl:Functional]
    inverse_of: producedBatch

dataproperties:
  harvestDate:
    domain:   HarvestBatch
    datatype: xsd:date
```

### 2. Generation — `config/generation.yaml`

```yaml
num_entities: 5000
num_triples:  15000
multityping:  true
avg_depth_specific_class: 3.5
relation_balance_ratio:   0.75

relation_distribution:
  type: pareto
  alpha: 1.2
  hotspots:
    - { relation: carriesBatch, multiplier: 3.0 }
    - { relation: shippedBy,    multiplier: 2.2 }

popularity_skew:
  default: { distribution: uniform }
  class_overrides:
    LogisticsProvider: { distribution: pareto, alpha: 1.1, head_boost: 2.5 }
```

### 3. Value Profiles — `config/value_profiles.yaml`

Each datatype property is mapped to one of 13 generator types — `truncated_normal`, `uniform`, `date_range`, `datetime_range`, `relation_offset`, `property_offset`, `ratio`, `code`, `choice`, `boolean`, `text`, `faker`, `blend_ratio` — enabling temporally-coherent dates, realistic codes, and weighted categorical values.

---

## Repository Layout

```
pygraft_sc_project/
├── cli/
│   └── pygraft_sc.py                   # Click-based CLI entry point
├── pygraft/
│   ├── core/
│   │   ├── schema_constructor.py       # TBox builder
│   │   ├── kg_generator.py             # ABox + supply-chain rule engine
│   │   ├── utils_schema.py             # YAML parsing & validation
│   │   └── utils.py                    # HermiT reasoner wrapper
│   ├── domains/supply_chain/
│   │   ├── ontology/                   # schema_{minimal,standard,extended}.yaml
│   │   ├── config/                     # generation.yaml, value_profiles.yaml
│   │   ├── constraints/                # SHACL shapes, OWL axioms
│   │   ├── pipelines/pipeline_supply_chain.py
│   │   └── providers/faker_providers.py
│   └── utils.py
├── output/                             # Generated artifacts (per run)
├── requirements.txt
├── setup.py
└── README.md
```

---

## Extending to a New Domain

1. Author a new ontology at `pygraft/domains/<domain>/ontology/schema_*.yaml`.
2. Add `config/generation.yaml` and `config/value_profiles.yaml` beside it.
3. Register domain-specific Faker providers in `providers/faker_providers.py`.
4. Implement a pipeline entry point mirroring `pipeline_supply_chain.py`.
5. *(Optional)* Add domain-specific rule-enforcement methods to `InstanceGenerator`.

The core generator, reasoner wrapper, and Value Profile Engine are domain-agnostic and reusable as-is.

---

## Acknowledgments

- Built on top of [PyGraft](https://github.com/nicolas-hbt/pygraft) by Nicolas Hubert et al.
- OWL reasoning provided by [HermiT](http://www.hermit-reasoner.com/).
- RDF tooling by [RDFLib](https://rdflib.readthedocs.io/) and [Owlready2](https://owlready2.readthedocs.io/).

---

## Citation

```bibtex
@mastersthesis{pygraft_sc_thesis,
  title  = {Artificial Generation of Graph Data Using Real-World Configurations},
  author = {Islam, Rakibul},
  school = {TU Chemnitz},
  year   = {2026}
}

@inproceedings{hubert2024pygraft,
  title     = {PyGraft: Configurable Generation of Synthetic Schemas and Knowledge Graphs at Your Fingertips},
  author    = {Hubert, Nicolas and others},
  booktitle = {ESWC},
  year      = {2024}
}
```

---

## License

MIT — see the upstream [PyGraft](https://github.com/nicolas-hbt/pygraft) repository for original licensing terms.

# Artificial Generation of Graph Data Using Real-World Configurations

Master's thesis project, TU Chemnitz.

This is an extension of [PyGraft](https://github.com/nicolas-hbt/pygraft) that generates synthetic knowledge graphs for the supply-chain domain. The original PyGraft produces graphs with generic labels like `C1`, `E1`, `R1`. My version takes a real ontology (farms, mills, harvest batches, shipments, logistics providers, retailers, etc.) written in YAML and produces an OWL-consistent RDF graph that also respects supply-chain business rules.

The generator is configuration-driven: you describe the ontology in one YAML file, the generation parameters in another, and how literal values should look in a third. Given the same random seed, the same input produces the same graph every time.

## What it does

Starting from three YAML files, the pipeline:

1. Parses the ontology and writes it out as an OWL file (`schema.rdf`).
2. Runs HermiT on the ontology to check it is consistent.
3. Creates typed entities, samples object-property triples, and checks each triple against the OWL axioms before keeping it.
4. Applies supply-chain specific rules — single grower per harvest batch, no circular shipments, logistics providers can't manage farms, one owner per facility, mandatory harvest links, etc.
5. Fills in literal values (dates, codes, weights, names) using value profiles, with temporal dependencies where they make sense (for example, a shipment date is derived from its batch's harvest date).
6. Serializes the final graph to Turtle, RDF/XML, N-Triples, or JSON-LD, and optionally reasons over it again.

## Requirements

- Python 3.8 or newer
- Java (only if you want HermiT to check consistency — you can skip it)

## Install

```bash
git clone <your-repo-url>
cd pygraft_sc_project

python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

The project installs in editable mode, so edits under `pygraft/` take effect without reinstalling.

## Run

The default run uses the standard supply-chain schema and writes Turtle:

```bash
python -m cli.pygraft_sc --format ttl --out supply_chain_kg.ttl
```

Skip the reasoner for a much faster run:

```bash
python -m cli.pygraft_sc \
    --schema pygraft/domains/supply_chain/ontology/schema_minimal.yaml \
    --skip-consistency --format ttl --seed 42 --out demo_minimal.ttl
```

Use the richer schema:

```bash
python -m cli.pygraft_sc \
    --schema pygraft/domains/supply_chain/ontology/schema_extended.yaml \
    --format ttl --seed 42 --out demo_extended.ttl
```

### CLI flags

- `--schema` — path to the schema YAML (defaults to `schema_standard.yaml`)
- `--format` — one of `ttl`, `json-ld`, `nt`, `rdf`, `xml`
- `--seed` — random seed (default 42)
- `--out` — file to write the final graph to
- `--check-consistency` / `--skip-consistency` — turn HermiT on or off (on by default)

## Schemas

There are three schema variants, so you can pick whichever matches the complexity you want to show:

| Variant  | Classes | Object properties | Datatype properties |
|----------|--------:|------------------:|--------------------:|
| Minimal  | 4       | 5                 | 3                   |
| Standard | 11      | 12                | 8                   |
| Extended | 25      | 40                | 15                  |

They all live under `pygraft/domains/supply_chain/ontology/`.

## Example run

Numbers below come from the committed artifacts in `output/`, generated with the default config and seed 42.

| Schema                  | Entities | Relations used | Triples |
|-------------------------|---------:|---------------:|--------:|
| `supply_chain_minimal`  | 3,664    | 5              | 4,455   |
| `supply_chain`          | 2,271    | 11             | 2,424   |
| `supply_chain_extended` | 5,493    | 40             | 14,491  |

And a small Turtle snippet of what the output looks like:

```turtle
sc:Entity_42 a sc:HarvestBatch ;
    rdfs:label          "Batch Oakridge-2024-07" ;
    sc:harvestedFrom    sc:Entity_17 ;
    sc:harvestDate      "2024-07-14"^^xsd:date ;
    sc:harvestWeightKg  "1342.5"^^xsd:decimal ;
    sc:qualityGrade     "A" .

sc:Entity_91 a sc:Shipment ;
    sc:carriesBatch   sc:Entity_42 ;
    sc:shippedFrom    sc:Entity_17 ;
    sc:shippedTo      sc:Entity_58 ;
    sc:shippedBy      sc:Entity_33 ;
    sc:shipmentDate   "2024-07-21"^^xsd:date ;
    sc:shipmentStatus "in_transit" .
```

## Output

Every run writes the final graph twice: once to the `--out` path you give on the command line, and once to a structured folder under `output/<schema_name>/`:

```
output/supply_chain/
├── schema.rdf              # OWL ontology (TBox)
├── class_info.json         # Parsed class hierarchy + disjointness
├── relation_info.json      # Object properties + OWL characteristics
├── dataproperty_info.json  # Datatype properties
├── kg_info.json            # Run metrics
└── full_graph.<ext>        # Final graph (TBox + ABox)
```

The JSON files are checkpoints produced by the schema stage and read by the instance generator. They are what makes the two stages of the pipeline loosely coupled.

## Configuration

Three YAML files drive the generator. They sit under `pygraft/domains/supply_chain/`.

**`ontology/schema_*.yaml`** — the ontology itself. Classes, their parent and disjointness, object properties with their domain/range/characteristics, datatype properties and their XSD type.

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

**`config/generation.yaml`** — how big the graph should be and how skewed the distributions are. Entity count, triple count, multi-typing depth, relation hotspots, per-class popularity curves (uniform, Pareto, Zipf), whether to run HermiT.

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

**`config/value_profiles.yaml`** — how each literal is generated. Each datatype property maps to a profile type: `truncated_normal`, `uniform`, `date_range`, `datetime_range`, `relation_offset`, `property_offset`, `ratio`, `code`, `choice`, `boolean`, `text`, `faker`, or `blend_ratio`. The offset profiles are what let a shipment date depend on its batch's harvest date instead of being random.

## Repository layout

```
pygraft_sc_project/
├── cli/
│   └── pygraft_sc.py                 # CLI entry point
├── pygraft/
│   ├── core/
│   │   ├── schema_constructor.py     # Builds the OWL ontology
│   │   ├── kg_generator.py           # Generates instances + applies SC rules
│   │   ├── utils_schema.py           # YAML parsing and validation
│   │   └── utils.py                  # HermiT wrapper, helpers
│   ├── domains/supply_chain/
│   │   ├── ontology/                 # schema_{minimal,standard,extended}.yaml
│   │   ├── config/                   # generation.yaml, value_profiles.yaml
│   │   ├── constraints/              # SHACL shapes, OWL axioms
│   │   ├── pipelines/pipeline_supply_chain.py
│   │   └── providers/faker_providers.py
│   └── utils.py
├── output/                           # Written by each run
├── requirements.txt
├── setup.py
└── README.md
```

## Using it for another domain

The core of the generator is not tied to supply chains — that logic lives in `pygraft/domains/supply_chain/` plus the supply-chain-specific methods in `kg_generator.py`. To adapt it to a different domain you would:

1. Write a new ontology under `pygraft/domains/<your_domain>/ontology/`.
2. Add a `generation.yaml` and `value_profiles.yaml` next to it.
3. Add any Faker providers you want for readable labels.
4. Copy `pipeline_supply_chain.py` as a starting point and wire it up.

Rule-enforcement methods are optional — without them you still get an OWL-consistent graph; with them you get domain-consistent data.

## Credits

- Built on top of [PyGraft](https://github.com/nicolas-hbt/pygraft) by Nicolas Hubert et al.
- OWL reasoning: [HermiT](http://www.hermit-reasoner.com/).
- RDF libraries: [RDFLib](https://rdflib.readthedocs.io/) and [Owlready2](https://owlready2.readthedocs.io/).

## Citation

```bibtex
@mastersthesis{islam2026graphdata,
  title  = {Artificial Generation of Graph Data Using Real-World Configurations},
  author = {Islam, Rakibul},
  school = {TU Chemnitz},
  year   = {2026}
}
```

## License

MIT. See the upstream PyGraft repository for the original licensing terms.

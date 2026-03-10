# PyGraft Supply Chain 

## Architecture Overview

PyGraft-SC generates synthetic Knowledge Graphs (KGs) for supply chain domains from YAML schema definitions. The pipeline flows:

```
schema.yaml → SchemaBuilder → InstanceGenerator → full_graph.ttl (RDF/TTL output)
```

**Key layers:**

- **Input**: `pygraft/domains/supply_chain/ontology/schema.yaml` (classes, relations, dataproperties)
- **Config**: `pygraft/domains/supply_chain/config/` (generation.yaml, value_profiles.yaml)
- **Core**: `pygraft/core/` (schema_constructor.py, kg_generator.py, utils_schema.py)
- **Output**: `output/{schema_name}/` (class_info.json, relation_info.json, full_graph.ttl)

## Entry Points & Commands

```bash
# Primary CLI for supply chain KG generation
python cli/pygraft_sc.py --schema pygraft/domains/supply_chain/ontology/schema_minimal.yaml --format ttl --seed 42

# Skip HermiT reasoner consistency checking (faster)
python cli/pygraft_sc.py --skip-consistency

# Original PyGraft CLI (general-purpose)
python -m pygraft.core.main -conf config.yaml -g generate
```

## Schema Definition Pattern (schema.yaml)

Classes use single inheritance with `parents` and `disjoint_with`:

```yaml
classes:
  HarvestBatch:
    parents: [MaterialLot]
    disjoint_with: [Shipment]
```

Relations specify domain/range and OWL characteristics:

```yaml
relations:
  harvestedFrom:
    domain: HarvestBatch
    range: Farm
    characteristics: [owl:Functional]
    inverse_of: producedBatch
```

Dataproperties require explicit `datatype`:

```yaml
dataproperties:
  harvestDate:
    domain: HarvestBatch
    datatype: xsd:date
```

## Value Generation (value_profiles.yaml)

Literals use typed profiles - know these patterns when adding new properties:

- `type: faker` + `method: company` → Faker method calls
- `type: choice` + `values` → weighted random selection
- `type: truncated_normal` → bounded numerical values
- `type: relation_offset` → dates derived from related entities

## Critical Implementation Details

1. **Class hierarchy**: Single inheritance only. `direct_class2superclass` maps child→parent; `owl:Thing` is implicit root.

2. **Functional properties**: `InstanceGenerator` enforces `owl:Functional` constraints - one value per subject.

3. **Type inference**: Entities without explicit types get inferred from relation domain/range via `_infer_types_for_untyped_entities()`.

4. **Output artifacts**: Generation creates `output/{name}/` with:
   - `schema.rdf` - OWL ontology
   - `class_info.json`, `relation_info.json`, `dataproperty_info.json` - parsed schema
   - `full_graph.ttl` - generated instances

## Adding New Domain Classes

1. Add class definition in `schema.yaml` under `classes:`
2. If class needs custom labels, add provider method in `faker_providers.py`
3. Add any dataproperties with their value profiles
4. Update `generation.yaml` if class needs custom `popularity_skew`

## Namespace Convention

All entities use `http://pygraf.t/` namespace bound to `sc:` prefix. Entity URIs follow pattern `sc:Entity_{index}`.

## Dependencies

Core: `rdflib`, `owlready2`, `faker`, `pyyaml`, `click`. Reasoner uses HermiT via JPype (`jpype1`).

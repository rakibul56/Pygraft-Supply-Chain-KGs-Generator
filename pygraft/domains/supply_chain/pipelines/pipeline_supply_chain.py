from pathlib import Path
import random
import numpy as np
import yaml
from pygraft.domains.supply_chain.providers.faker_providers import make_faker
from pygraft.core.schema_constructor import SchemaBuilder
from pygraft.core.kg_generator import InstanceGenerator
from pygraft.core.utils_schema import load_schema_yaml
from pygraft.utils import set_reasoner_java_memory_mb

BASE = Path(__file__).resolve().parents[2] / "supply_chain"
OUTPUT_DIR = Path("output")


def load_generation_config(path: Path) -> dict:
    with path.open() as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("Generation config must be a mapping")
    return data


def ensure_required(config: dict, keys: list[str]):
    missing = [key for key in keys if key not in config]
    if missing:
        raise KeyError(f"Missing required config keys: {', '.join(missing)}")


def load_value_profiles(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open() as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Value profile config must be a mapping")
    return data


def build_supply_chain_graph(
    out_format: str = "ttl",
    seed: int = 42,
    enable_consistency_checking: bool = True,
    schema_path: Path | str | None = None,
) -> str:
    schema_file = Path(schema_path) if schema_path else BASE / "ontology" / "schema_standard.yaml"
    schema_payload = load_schema_yaml(schema_file)
    schema_meta = schema_payload["schema"]
    schema_name = schema_meta["name"]
    schema_format = schema_meta.get("format", "xml")

    config = load_generation_config(BASE / "config" / "generation.yaml")
    ensure_required(config, ["num_entities", "num_triples"])
    set_reasoner_java_memory_mb(config.get("reasoner_java_memory_mb"))

    builder = SchemaBuilder(
        schema_payload["class_info"],
        schema_payload["relation_info"],
        folder_name=schema_name,
        format=schema_format,
        dataproperty_info=schema_payload.get("dataproperty_info"),
    )
    builder.building_pipeline()

    config["schema_name"] = schema_name
    config["format"] = out_format or config.get("format", schema_format)
    config["kg_check_reasoner"] = enable_consistency_checking if enable_consistency_checking is not None else config.get("kg_check_reasoner", True)

    value_profiles = load_value_profiles(BASE / "config" / "value_profiles.yaml")

    np.random.seed(seed)
    random.seed(seed)

    faker = make_faker()
    faker.seed_instance(seed)

    generator = InstanceGenerator(
        schema=config["schema_name"],
        num_entities=config["num_entities"],
        num_triples=config["num_triples"],
        relation_balance_ratio=config.get("relation_balance_ratio"),
        relation_distribution=config.get("relation_distribution"),
        fast_gen=config.get("fast_gen"),
        oversample=config.get("oversample"),
        prop_untyped_entities=config.get("prop_untyped_entities"),
        avg_depth_specific_class=config.get("avg_depth_specific_class"),
        multityping=config.get("multityping"),
        avg_multityping=config.get("avg_multityping"),
        format=config.get("format", schema_format),
        kg_check_reasoner=config.get("kg_check_reasoner", enable_consistency_checking),
        popularity_skew=config.get("popularity_skew"),
        value_profiles=value_profiles,
        dataproperty_priority=config.get("dataproperty_priority"),
        faker=faker,
    )
    generator.generate_kg()

    extension = "rdf" if generator.format == "xml" else generator.format
    kg_path = OUTPUT_DIR / config["schema_name"] / f"full_graph.{extension}"
    return kg_path.read_text(encoding="utf-8")

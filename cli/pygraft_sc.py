from pathlib import Path
import click
from pygraft.domains.supply_chain.pipelines.pipeline_supply_chain import build_supply_chain_graph

@click.command()
@click.option("--format", "fmt", default="ttl", type=click.Choice(["ttl","json-ld","nt","rdf","xml"]))
@click.option("--schema", "schema_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), help="Path to a schema YAML file (e.g., schema_minimal.yaml or schema_standard.yaml)")
@click.option("--seed", default=42, type=int)
@click.option("--out", default="supply_chain_kg.ttl")
@click.option("--check-consistency/--skip-consistency", default=True, show_default=True, help="Toggle HermiT consistency checking during generation")
def main(fmt, schema_path, seed, out, check_consistency):
    data = build_supply_chain_graph(out_format=fmt, seed=seed, enable_consistency_checking=check_consistency, schema_path=schema_path)
    with open(out, "w", encoding="utf-8") as f:
        f.write(data)
    click.echo(f"Wrote {out}")

if __name__ == "__main__":
    main()

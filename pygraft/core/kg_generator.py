import copy
import json
import itertools
import random
import re
import time
from collections import Counter, defaultdict
import datetime as dt
from decimal import Decimal
import numpy as np
from owlready2 import *
from rdflib import Graph as RDFGraph, Namespace, URIRef, RDF, OWL, Literal
from rdflib.namespace import RDFS, XSD
from tqdm.auto import tqdm
from pygraft.utils_kg import *
from pygraft.utils import reasoner


class InstanceGenerator:
    def __init__(self, **kwargs):
        self.faker = kwargs.get('faker')
        self.entity_labels = {}
        self.init_params(**kwargs)
        self.init_utils(**kwargs)

    def init_params(self, **kwargs):
        """
        Initializes general KG information with user-specified parameters.

        Args:
            self (object): The instance of the InstanceGenerator.
            kwargs (dict): Dictionary of parameter names and values.

        Returns:
            None
        """
        self.schema = kwargs.get("schema")
        self.num_entities = kwargs.get("num_entities")
        self.num_triples = kwargs.get("num_triples")
        self.relation_balance_ratio = kwargs.get("relation_balance_ratio")
        self.relation_distribution = kwargs.get("relation_distribution") or {}
        self.prop_untyped_entities = kwargs.get("prop_untyped_entities")
        self.avg_depth_specific_class = kwargs.get("avg_depth_specific_class")
        self.multityping = kwargs.get("multityping")
        self.avg_multityping = kwargs.get("avg_multityping")
        self.multityping = False if self.avg_multityping == 0.0 else self.multityping
        self.kg_check_reasoner = kwargs.get("kg_check_reasoner")
        self.popularity_skew = kwargs.get("popularity_skew") or {}
        self.value_profiles = kwargs.get("value_profiles") or {}
        self.dataproperty_priority = kwargs.get("dataproperty_priority") or {}

    def init_utils(self, **kwargs):
        """
        Initializes auxiliary information.

        Args:
            self (object): The instance of the InstanceGenerator.
            kwargs (dict): Dictionary of parameter names and values.

        Returns:
            None
        """
        self.directory = f"output/{self.schema}/"
        self.format = kwargs.get("format")
        self.fast_gen = kwargs.get("fast_gen")
        self.oversample = kwargs.get("oversample")
        self.fast_ratio = get_fast_ratio(self.num_entities) if self.fast_gen else 1
        self.oversample_every = int(self.num_triples / self.fast_ratio)
        self.load_schema_info()

    def load_schema_info(self):
        """
        Loads schema information from class_info, dataproperty_info and relation_info json files.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """

        with open(f"{self.directory}class_info.json", "r") as file:
            self.class_info = json.load(file)
        with open(f"{self.directory}relation_info.json", "r") as file:
            self.relation_info = json.load(file)
        try:
            with open(f"{self.directory}dataproperty_info.json", "r") as file:
                self.dataproperty_info = json.load(file)
        except FileNotFoundError:
            self.dataproperty_info = {}

        self.functional_relations = set(self.relation_info.get("functional_relations", []))
        self.inversefunctional_relations = set(self.relation_info.get("inversefunctional_relations", []))
        self.rel2superrel = self.relation_info.get("rel2superrel", {})
        self.functional_dataproperties = set(self.dataproperty_info.get("functional_dataproperties", []))
        self.dp2dom = self.dataproperty_info.get("dp2dom", {})
        self.dp2datatype = self.dataproperty_info.get("dp2datatype", {})

        max_depth = max(1, self.class_info["hierarchy_depth"])
        if self.avg_depth_specific_class > max_depth:
            self.avg_depth_specific_class = max_depth

    def assemble_instance_info(self):
        """
        Assembles the KG information and returns a dictionary
        containing statistics and user parameters.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            kg_info (dict): A dictionary containing information about the KG.
        """
        observed_entities = {entity for tup in self.kg for entity in tup[::2]}
        typed_observed = {entity for entity in observed_entities if entity in self.ent2classes_specific}
        observed_relations = {tup[1] for tup in self.kg}
        kg_info = {
            "user_parameters": {
                "schema": self.schema,
                "num_entities": self.num_entities,
                "num_triples": self.num_triples,
                "relation_balance_ratio": self.relation_balance_ratio,
                "prop_untyped_entities": self.prop_untyped_entities,
                "avg_depth_specific_class": self.avg_depth_specific_class,
                "multityping": self.multityping,
                "avg_multityping": self.avg_multityping,
            },
            "statistics": {
                "num_entities": len(observed_entities),
                "num_instantiated_relations": len(observed_relations),
                "num_triples": len(self.kg),
                "prop_untyped_entities": round(1 - (len(typed_observed) / len(observed_entities)), 2),
                "avg_depth_specific_class": self.current_avg_depth_specific_class,
                "avg_multityping": round(self.calculate_avg_multityping(), 2) if len(self.is_typed) > 0 else 0.0,
            },
        }

        with open(self.directory + "kg_info.json", "w") as file:
            json.dump(kg_info, file, indent=4)

        return kg_info

    def write_kg(self):
        """
        Writes the KG to a file.
        Initializes a new RDFGraph object and parses the schema.
        Each triple in the KG is added to the full graph.
        The full graph is then serialized to a file and checked for consistency.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            str(kg_file): The resulting KG file path, e.g. 'output/template/full_graph.rdf'.
        """
        self.graph = RDFGraph()
        
        # Check which schema file exists and parse accordingly
        import os
        schema_rdf_path = f"{self.directory}schema.rdf"
        schema_format_path = f"{self.directory}schema.{self.format}"
        
        if os.path.exists(schema_rdf_path):
            self.graph.parse(schema_rdf_path, format="xml")
        elif os.path.exists(schema_format_path):
            self.graph.parse(schema_format_path, format=self.format)
        else:
            raise FileNotFoundError(f"Schema file not found. Looked for {schema_rdf_path} or {schema_format_path}")

        schema = Namespace("http://pygraf.t/")
        geo_ns = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        
        self.graph.namespace_manager.bind("sc", schema, replace=True)
        self.graph.namespace_manager.bind("geo", geo_ns, replace=True)

        # Collect all entities used in triples
        entities_in_triples = set()
        for h, r, t in self.kg:
            entities_in_triples.add(h)
            entities_in_triples.add(t)
        entities_in_triples.update(getattr(self, "literal_values_raw", {}).keys())

        # Infer types for untyped entities based on domain/range
        self._infer_types_for_untyped_entities(entities_in_triples)

        def _filtered_classes(class_set):
            return {c for c in class_set if c not in {"owl:Thing", "Entity"}}

        for h, r, t in tqdm(self.kg, desc="Writing instance triples", unit="triples", colour="red"):
            self.graph.add((URIRef(schema + h), URIRef(schema + r), URIRef(schema + t)))

            classes_h = _filtered_classes(set(self.ent2classes_specific.get(h, [])) | set(self.ent2classes_transitive.get(h, [])))
            for c in classes_h:
                self.graph.add((URIRef(schema + h), RDF.type, URIRef(schema + c)))

            classes_t = _filtered_classes(set(self.ent2classes_specific.get(t, [])) | set(self.ent2classes_transitive.get(t, [])))
            for c in classes_t:
                self.graph.add((URIRef(schema + t), RDF.type, URIRef(schema + c)))

        for ent, label in self.entity_labels.items():
            if label and ent in entities_in_triples:
                self.graph.add((URIRef(schema + ent), RDFS.label, Literal(label)))

        dataprop_datatypes = getattr(self, "dp2datatype", {})
        for ent, prop_map in getattr(self, "literal_values_raw", {}).items():
            ent_uri = URIRef(schema + ent)
            classes_ent = _filtered_classes(set(self.ent2classes_specific.get(ent, [])) | set(self.ent2classes_transitive.get(ent, [])))
            for cls in classes_ent:
                self.graph.add((ent_uri, RDF.type, URIRef(schema + cls)))
            for prop, values in prop_map.items():
                datatype = dataprop_datatypes.get(prop)
                config = self.value_profiles.get(prop, {})
                prop_uri = URIRef(schema + prop)
                for value in values:
                    literal = self._build_literal(value, datatype, config)
                    if literal is not None:
                        self.graph.add((ent_uri, prop_uri, literal))

        # Enrich with WGS84 lat/long for facilities that need spatial context
        for ent, (lat, lon) in self._generate_geo_coordinates().items():
            ent_uri = URIRef(schema + ent)
            self.graph.add((ent_uri, geo_ns.lat, Literal(lat, datatype=XSD.decimal)))
            self.graph.add((ent_uri, geo_ns.long, Literal(lon, datatype=XSD.decimal)))

        self.graph.serialize(
            f"{self.directory}full_graph.rdf", format="xml"
        ) if self.format == "xml" else self.graph.serialize(
            f"{self.directory}full_graph.{self.format}", format=self.format
        )

        kg_file = (
            f"{self.directory}full_graph.rdf" if self.format == "xml" else f"{self.directory}full_graph.{self.format}"
        )
        return kg_file

    def _build_relation_index(self):
        """
        Build and return indexed lookups to avoid repeated full-KG scans.
        Returns:
            rel_head: dict[(head, rel)] -> list of tails
            rel_tail: dict[(tail, rel)] -> list of heads
            rel2triples: dict[rel] -> list of (h, t)
        """
        rel_head = defaultdict(list)
        rel_tail = defaultdict(list)
        rel2triples = defaultdict(list)
        for h, r, t in self.kg:
            rel_head[(h, r)].append(t)
            rel_tail[(t, r)].append(h)
            rel2triples[r].append((h, t))
        return rel_head, rel_tail, rel2triples

    def _infer_types_for_untyped_entities(self, entities_in_triples):
        """
        Infer types for entities that appear in triples but lack type declarations.
        Uses domain and range constraints from relations to assign appropriate types.

        Args:
            self (object): The instance of the InstanceGenerator.
            entities_in_triples (set): Set of entity identifiers used in the KG triples.

        Returns:
            None
        """
        # Find entities without types
        untyped_in_triples = entities_in_triples - set(self.ent2classes_specific.keys())
        
        if not untyped_in_triples:
            return
        
        print(f"\nInferring types for {len(untyped_in_triples)} untyped entities...")
        
        # Build a map: entity -> possible types based on usage
        entity_to_inferred_types = {}
        
        for h, r, t in self.kg:
            # Infer type for head entity from domain
            if h in untyped_in_triples:
                domain_class = self.rel2dom.get(r)
                if domain_class:
                    if h not in entity_to_inferred_types:
                        entity_to_inferred_types[h] = set()
                    entity_to_inferred_types[h].add(domain_class)
            
            # Infer type for tail entity from range
            if t in untyped_in_triples:
                range_class = self.rel2range.get(r)
                if range_class:
                    if t not in entity_to_inferred_types:
                        entity_to_inferred_types[t] = set()
                    entity_to_inferred_types[t].add(range_class)
        
        # Assign the inferred types to entities
        for entity, possible_types in entity_to_inferred_types.items():
            if possible_types:
                # Pick the most specific type (or first one if multiple)
                # Prefer leaf classes over parent classes
                chosen_type = self._choose_most_specific_type(list(possible_types))
                
                # Add to ent2classes_specific
                self.ent2classes_specific[entity] = [chosen_type]
                
                # Add transitive superclasses
                transitive_classes = [chosen_type] + self.class_info["transitive_class2superclasses"].get(chosen_type, [])
                self.ent2classes_transitive[entity] = transitive_classes
                
                # Update is_typed set
                self.is_typed.add(entity)
        
        # For entities that still have no inferred type, assign a default type
        still_untyped = untyped_in_triples - set(entity_to_inferred_types.keys())
        if still_untyped:
            default_type = self._get_default_type_for_untyped()
            for entity in still_untyped:
                self.ent2classes_specific[entity] = [default_type]
                transitive_classes = [default_type] + self.class_info["transitive_class2superclasses"].get(default_type, [])
                self.ent2classes_transitive[entity] = transitive_classes
                self.is_typed.add(entity)
        
        typed_count = len(entity_to_inferred_types)
        default_typed_count = len(still_untyped)
        
        print(f"Successfully inferred types for {typed_count} entities from domain/range constraints.")
        if default_typed_count > 0:
            print(f"Assigned default type '{default_type}' to {default_typed_count} entities without constraints.")

    def _choose_most_specific_type(self, candidate_types):
        """
        Choose the most specific type from a list of candidate types.
        Prefers leaf classes (classes with no subclasses).

        Args:
            self (object): The instance of the InstanceGenerator.
            candidate_types (list): List of candidate class names.

        Returns:
            str: The chosen class name.
        """
        if len(candidate_types) == 1:
            return candidate_types[0]
        
        # Find leaf classes (no subclasses)
        leaf_classes = []
        for cls in candidate_types:
            subclasses = self.class_info["direct_class2subclasses"].get(cls, [])
            if not subclasses:
                leaf_classes.append(cls)
        
        # Return first leaf class, or first candidate if no leaf found
        return leaf_classes[0] if leaf_classes else candidate_types[0]

    def _get_default_type_for_untyped(self):
        """
        Get a default type for entities that couldn't be typed through domain/range inference.
        Chooses a common, general type like 'Organization' or 'Entity'.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            str: The default class name to use.
        """
        # Priority order: prefer Organization for supply chain, then Entity, then any leaf class
        preferred_defaults = ["Organization", "Supplier", "Entity"]
        
        for default in preferred_defaults:
            if default in self.classes:
                return default
        
        # If none of the preferred defaults exist, pick the first leaf class
        for cls in self.classes:
            subclasses = self.class_info["direct_class2subclasses"].get(cls, [])
            if not subclasses and cls != "Entity":  # Avoid Entity if possible
                return cls
        
        # Last resort: return the first available class
        return self.classes[0] if self.classes else "Entity"

    def generate_kg(self):
        self.pipeline()
        self._enforce_org_roles()
        self._ensure_logistics_providers()
        self.enforce_supply_chain_rules()
        self._ensure_harvest_batch_links()
        self._generate_product_batches()
        self._seed_batch_shipments()
        self.check_asymmetries()
        self.check_inverseof_asymmetry()
        self.check_dom_range()
        self.procedure_1()
        self.procedure_2()
        self._assign_dataproperty_values()
        self._group_shipments_by_route()
        self._prune_orphan_facilities()
        self._ensure_harvest_batch_links(strict=True)
        kg_info = self.assemble_instance_info()
        kg_file = self.write_kg()
        if self.kg_check_reasoner:
            reasoner(resource_file=kg_file, resource="KG")
        else:
            print(f"\nSkipping the KG check step with reasoning.\n")

    def enforce_supply_chain_rules(self):
        """
        Apply domain-specific constraints for supply chain generation.
        Ensures non-circular shipments, chain of custody, limited ownership clustering,
        and role separation between growers and mill operators.
        """
        to_remove = set()
        to_add = set()

        rel_head, rel_tail, rel2triples = self._build_relation_index()

        # Enforce unique batch ownership: one Grower per HarvestBatch
        hb_growers = defaultdict(list)
        for grower, batch in rel2triples.get("harvestsBatch", []):
            hb_growers[batch].append(grower)
        for batch, growers_list in hb_growers.items():
            if len(growers_list) > 1:
                keep = growers_list[0]
                # Remove extra ownerships and create unique batches per extra grower
                for idx, grower in enumerate(growers_list[1:], start=1):
                    to_remove.add((grower, "harvestsBatch", batch))
                    # Create a unique batch for this grower
                    new_batch = f"{batch}_G{idx}"
                    while new_batch in self.entities:
                        new_batch += "X"
                    self.entities.append(new_batch)
                    self.is_typed.add(new_batch)
                    # Typing for the new batch
                    self.ent2classes_specific[new_batch] = ["HarvestBatch"]
                    supers = self.class_info["transitive_class2superclasses"].get("HarvestBatch", [])
                    self.ent2classes_transitive[new_batch] = list(set(["HarvestBatch"] + list(supers)))
                    self.class2entities.setdefault("HarvestBatch", []).append(new_batch)
                    for sup in supers:
                        self.class2entities.setdefault(sup, [])
                        if new_batch not in self.class2entities[sup]:
                            self.class2entities[sup].append(new_batch)
                    # Copy core location relations from the original batch if present
                    for rel in ("harvestedFrom", "processedAt"):
                        locs = rel_head.get((batch, rel), [])
                        if locs:
                            loc = locs[0]
                            if self.check_consistency((new_batch, rel, loc)):
                                to_add.add((new_batch, rel, loc))
                    # Carry over dataproperties if needed
                    if hasattr(self, "literal_values_raw") and batch in self.literal_values_raw:
                        self.literal_values_raw[new_batch] = copy.deepcopy(self.literal_values_raw[batch])
                    # Assign unique harvestsBatch to the new grower
                    to_add.add((grower, "harvestsBatch", new_batch))

        shipments = set(self.class2entities.get("Shipment", []))
        facility_classes = set(self.class_info.get("transitive_class2subclasses", {}).get("Facility", []))
        facility_classes.add("Facility")
        facilities = set()
        for cls in facility_classes:
            facilities.update(self.class2entities.get(cls, []))
        growers = set(self.class2entities.get("Grower", []))
        retailers = set(self.class2entities.get("Retailer", []))
        organizations = set(self.class2entities.get("Organization", []))
        logistics_providers = set(self.class2entities.get("LogisticsProvider", []))

        manages_farm_heads = {h for h, r, _ in self.kg if r == "managesFarm"}
        operates_mill_triples = {(h, r, t) for h, r, t in self.kg if r == "operatesMill"}
        manages_farm_triples = {(h, r, t) for h, r, t in self.kg if r == "managesFarm"}

        if logistics_providers:
            facility_relations = {rel for rel, rng in self.rel2range.items() if rng in facility_classes}
            for rel in facility_relations:
                for h, t in rel2triples.get(rel, []):
                    if h in logistics_providers:
                        to_remove.add((h, rel, t))
                        if rel == "ownsFacility":
                            to_remove.add((t, "ownedBy", h))
            for h, t in rel2triples.get("ownedBy", []):
                if t in logistics_providers:
                    to_remove.add((h, "ownedBy", t))
                    to_remove.add((t, "ownsFacility", h))

        def first_relation(head, rel):
            vals = rel_head.get((head, rel))
            return vals[0] if vals else None

        def batch_location(batch):
            """Return last known location: prefer processedAt mill, else harvestedFrom farm."""
            loc_processed = first_relation(batch, "processedAt")
            if loc_processed:
                return loc_processed
            return first_relation(batch, "harvestedFrom")

        def sample_facility(exclude=None):
            pool = [f for f in facilities if f != exclude]
            if not pool:
                return None
            return np.random.choice(pool)

        def make_shipment_id():
            base_idx = len(self.entities) + 1
            candidate = f"Shipment_IN_{base_idx}"
            used = set(self.entities)
            while candidate in used:
                base_idx += 1
                candidate = f"Shipment_IN_{base_idx}"
            return candidate

        # Inbound shipments: Farm -> Mill for processed batches
        inbound_candidates = []
        harvest_batches = self.class2entities.get("HarvestBatch", [])
        for batch in harvest_batches:
            mill = first_relation(batch, "processedAt")
            farm = first_relation(batch, "harvestedFrom")
            if mill and farm:
                inbound_candidates.append((batch, farm, mill))

        # track existing carries for quick lookup
        carries = {(h, t) for h, r, t in self.kg if r == "carriesBatch"}

        for batch, farm, mill in inbound_candidates:
            existing_sh = [h for h, t in carries if t == batch]
            already_has_inbound = False
            for sh in existing_sh:
                from_vals = rel_head.get((sh, "shippedFrom"), [])
                to_vals = rel_head.get((sh, "shippedTo"), [])
                if farm in from_vals and mill in to_vals:
                    already_has_inbound = True
                    break
            if already_has_inbound:
                continue

            new_sh = make_shipment_id()
            self.entities.append(new_sh)
            self.is_typed.add(new_sh)
            if "Shipment" in self.class_info.get("classes", []):
                self.ent2classes_specific[new_sh] = ["Shipment"]
                supers = self.class_info["transitive_class2superclasses"].get("Shipment", [])
                self.ent2classes_transitive[new_sh] = list(set(["Shipment"] + list(supers)))
                self.class2entities.setdefault("Shipment", []).append(new_sh)
                shipments.add(new_sh)

            to_add.update(
                {
                    (new_sh, "carriesBatch", batch),
                    (new_sh, "shippedFrom", farm),
                    (new_sh, "shippedTo", mill),
                }
            )

        # Chain of custody & circular shipment guards
        for sh in shipments:
            batches = set(rel_head.get((sh, "carriesBatch"), []))
            batches.update(rel_tail.get((sh, "includedInShipment"), []))

            target_from = None
            processed_locs = [batch_location(b) for b in batches if batch_location(b)]
            if processed_locs:
                # Prefer any processedAt location
                target_from = next((loc for loc in processed_locs if loc in self.class2entities.get("Mill", [])), processed_locs[0])
            else:
                for batch in batches:
                    loc = batch_location(batch)
                    if loc:
                        target_from = loc
                        break

            existing_from_vals = rel_head.get((sh, "shippedFrom"), [])
            existing_to_vals = rel_head.get((sh, "shippedTo"), [])

            source = target_from
            if not source and existing_from_vals:
                source = existing_from_vals[0]
            if not source:
                source = sample_facility()

            if source:
                for t in existing_from_vals:
                    if t != source:
                        to_remove.add((sh, "shippedFrom", t))
                if (sh, "shippedFrom", source) not in self.kg:
                    to_add.add((sh, "shippedFrom", source))

            dest = None
            if existing_to_vals:
                dest = next((d for d in existing_to_vals if d != source), existing_to_vals[0])
            if not dest:
                dest = sample_facility(exclude=source)
            if dest == source:
                alt = sample_facility(exclude=source)
                if alt and alt != source:
                    dest = alt
                else:
                    dest = None

            if dest:
                for t in existing_to_vals:
                    if t != dest:
                        to_remove.add((sh, "shippedTo", t))
                if (sh, "shippedTo", dest) not in self.kg:
                    to_add.add((sh, "shippedTo", dest))

        # Role separation: only growers manage farms; only non-grower orgs operate mills
        for h, r, t in manages_farm_triples:
            if h in logistics_providers:
                to_remove.add((h, r, t))
                if growers:
                    replacement = np.random.choice(list(growers))
                    to_add.add((replacement, "managesFarm", t))
                continue
            if h not in growers:
                to_remove.add((h, r, t))
                if growers:
                    replacement = np.random.choice(list(growers))
                    to_add.add((replacement, "managesFarm", t))
                # force class
                if "Grower" in self.class2entities:
                    self.class2entities["Grower"].append(h)
                    self.ent2classes_specific[h] = ["Grower"]

        operator_pool = [
            org
            for org in organizations
            if org not in growers and org not in manages_farm_heads and org not in logistics_providers
        ]
        preferred_ops = [org for org in retailers if org in operator_pool] if retailers else []

        for h, _, t in operates_mill_triples:
            needs_replace = h in growers or h in manages_farm_heads or h in logistics_providers or (preferred_ops and h not in preferred_ops)
            if needs_replace:
                to_remove.add((h, "operatesMill", t))
                replacement = None
                if preferred_ops:
                    replacement = np.random.choice(preferred_ops)
                elif operator_pool:
                    replacement = np.random.choice(operator_pool)
                if replacement:
                    to_add.add((replacement, "operatesMill", t))
                # force class for replacement
                if replacement and "Retailer" in self.class2entities:
                    self.class2entities["Retailer"].append(replacement)
                    self.ent2classes_specific[replacement] = ["Retailer"]

        if to_remove:
            self._remove_triples(to_remove)

        for triple in to_add:
            if self.check_consistency(triple):
                if triple not in self.kg:
                    self.kg.add(triple)
                self._register_functional_usage(*triple)

        # Enforce single owner per facility across ownsFacility and its subproperties
        rel_head, rel_tail, rel2triples = self._build_relation_index()

        def is_ownership_relation(rel):
            if rel == "ownsFacility":
                return True
            current = rel
            while current in self.rel2superrel:
                current = self.rel2superrel[current]
                if current == "ownsFacility":
                    return True
            return False

        ownership_rels = {rel for rel in self.rel2superrel.keys() if is_ownership_relation(rel)}
        ownership_rels.add("ownsFacility")

        owners_by_facility = defaultdict(list)
        for rel in ownership_rels:
            for h, t in rel2triples.get(rel, []):
                owners_by_facility[t].append((h, rel))
        for h, t in rel2triples.get("ownedBy", []):
            owners_by_facility[h].append((t, "ownedBy"))

        to_remove = set()
        for fac, owners in owners_by_facility.items():
            unique_owners = []
            for owner, rel in owners:
                if owner not in unique_owners:
                    unique_owners.append(owner)
            if len(unique_owners) <= 1:
                continue
            keep_owner = unique_owners[0]
            for owner, rel in owners:
                if owner == keep_owner:
                    continue
                if rel == "ownedBy":
                    to_remove.add((fac, "ownedBy", owner))
                else:
                    to_remove.add((owner, rel, fac))
                # Remove any remaining ownership assertions for the dropped owner.
                to_remove.add((owner, "ownsFacility", fac))
                to_remove.add((fac, "ownedBy", owner))

        if to_remove:
            self._remove_triples(to_remove)

    def _ensure_logistics_providers(self, min_count=5, max_count=10):
        """Ensure dedicated LogisticsProvider entities exist for shipments."""
        if "LogisticsProvider" not in self.class_info.get("classes", []):
            return
        current = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        target = max(min_count, min(max_count, max(min_count, len(current))))
        needed = target - len(current)
        if needed <= 0:
            return

        supers = self.class_info["transitive_class2superclasses"].get("LogisticsProvider", [])
        new_entities = []
        next_id = len(self.entities) + 1
        for _ in range(needed):
            candidate = f"LP{next_id}"
            while candidate in self.entities:
                next_id += 1
                candidate = f"LP{next_id}"
            new_entities.append(candidate)
            next_id += 1

        self.entities.extend(new_entities)
        self.is_typed.update(new_entities)
        self.class2entities.setdefault("LogisticsProvider", [])
        self.class2entities["LogisticsProvider"].extend(new_entities)

        for lp in new_entities:
            self.ent2classes_specific[lp] = ["LogisticsProvider"]
            self.ent2classes_transitive[lp] = list(set(["LogisticsProvider"] + list(supers)))
            for sup in supers:
                self.class2entities.setdefault(sup, [])
                if lp not in self.class2entities[sup]:
                    self.class2entities[sup].append(lp)

        # refresh logistics pool index
        self.class2entities["LogisticsProvider"] = list(dict.fromkeys(self.class2entities["LogisticsProvider"]))

    def _ensure_harvest_batch_links(self, strict: bool = False):
        """Ensure HarvestBatch entities have harvestedFrom and harvestsBatch links."""
        classes = set(self.class_info.get("classes", []))
        if "HarvestBatch" not in classes:
            return
        if "harvestedFrom" not in self.rel2dom or "harvestsBatch" not in self.rel2dom:
            return

        farms = list(dict.fromkeys(self.class2entities.get("Farm", [])))
        growers = list(dict.fromkeys(self.class2entities.get("Grower", [])))
        if not farms or not growers:
            return

        def _has_type(ent, class_name):
            return class_name in self.ent2classes_transitive.get(ent, [])

        def _is_disjoint(ent, class_name):
            disjoint = set(self.class2disjoints_extended.get(class_name, []))
            return bool(disjoint.intersection(self.ent2classes_transitive.get(ent, [])))

        rel_head, rel_tail, rel2triples = self._build_relation_index()

        if strict:
            to_remove = set()
            for batch, farm in rel2triples.get("harvestedFrom", []):
                if not _has_type(farm, "Farm") or _is_disjoint(farm, "Farm"):
                    to_remove.add((batch, "harvestedFrom", farm))
            for grower, batch in rel2triples.get("harvestsBatch", []):
                if not _has_type(grower, "Grower") or _is_disjoint(grower, "Grower"):
                    to_remove.add((grower, "harvestsBatch", batch))
            if to_remove:
                self._remove_triples(to_remove)
                rel_head, rel_tail, rel2triples = self._build_relation_index()

        grower_to_farms = defaultdict(list)
        farm_to_growers = defaultdict(list)
        if "managesFarm" in rel2triples:
            for grower, farm in rel2triples.get("managesFarm", []):
                grower_to_farms[grower].append(farm)
                farm_to_growers[farm].append(grower)

        ownership_rels = {rel for rel, sup in self.rel2superrel.items() if sup == "ownsFacility"}
        ownership_rels.add("ownsFacility")
        owners_by_facility = defaultdict(list)
        for rel in ownership_rels:
            for owner, facility in rel2triples.get(rel, []):
                owners_by_facility[facility].append(owner)
        for facility, owner in rel2triples.get("ownedBy", []):
            owners_by_facility[facility].append(owner)
        for facility, owners in owners_by_facility.items():
            owners_by_facility[facility] = list(dict.fromkeys(owners))
            for owner in owners_by_facility[facility]:
                if owner in growers:
                    farm_to_growers[facility].append(owner)
                    grower_to_farms[owner].append(facility)

        harvest_batches = list(dict.fromkeys(self.class2entities.get("HarvestBatch", [])))
        def _add_relation(triple):
            if triple in self.kg:
                return True
            if not strict and not self.check_consistency(triple):
                return False
            if strict and not self.check_consistency(triple):
                # Clear stale inverse-functional usage entries before forcing.
                rel = triple[1]
                if rel in self.inversefunctional_usage:
                    self.inversefunctional_usage[rel].pop(triple[2], None)
            if strict or self.check_consistency(triple):
                self.kg.add(triple)
                self._register_functional_usage(*triple)
                return True
            return False

        for batch in harvest_batches:
            existing_farms = rel_head.get((batch, "harvestedFrom"), [])
            existing_growers = rel_tail.get((batch, "harvestsBatch"), [])

            farm = existing_farms[0] if existing_farms else None
            grower = existing_growers[0] if existing_growers else None

            if farm is None:
                if grower:
                    candidate_farms = grower_to_farms.get(grower)
                    if candidate_farms:
                        farm = np.random.choice(candidate_farms)
                if farm is None and farm_to_growers:
                    farm = np.random.choice(list(farm_to_growers.keys()))
                if farm is None:
                    farm = np.random.choice(farms)

            if grower is None:
                if farm:
                    candidate_growers = farm_to_growers.get(farm)
                    if candidate_growers:
                        grower = np.random.choice(candidate_growers)
                if grower is None and farm:
                    owner_candidates = [o for o in owners_by_facility.get(farm, []) if o in growers]
                    if owner_candidates:
                        grower = np.random.choice(owner_candidates)
                if grower is None:
                    grower = np.random.choice(growers)

            if grower and farm and "managesFarm" in self.rel2dom:
                existing_owners = owners_by_facility.get(farm, [])
                if not existing_owners or grower in existing_owners:
                    if not farm_to_growers.get(farm):
                        if _add_relation((grower, "managesFarm", farm)):
                            grower_to_farms[grower].append(farm)
                            farm_to_growers[farm].append(grower)
                            owners_by_facility[farm].append(grower)

            if farm and not existing_farms:
                _add_relation((batch, "harvestedFrom", farm))

            if grower and not existing_growers:
                _add_relation((grower, "harvestsBatch", batch))

        if strict:
            rel_head, rel_tail, rel2triples = self._build_relation_index()
            to_remove = set()

            for batch in harvest_batches:
                farm_links = rel_head.get((batch, "harvestedFrom"), [])
                valid_farms = [farm for farm in farm_links if farm in farms]
                for farm in farm_links:
                    if farm not in farms:
                        to_remove.add((batch, "harvestedFrom", farm))
                if not valid_farms and farms:
                    _add_relation((batch, "harvestedFrom", np.random.choice(farms)))

                grower_links = rel_tail.get((batch, "harvestsBatch"), [])
                valid_growers = [g for g in grower_links if g in growers]
                for g in grower_links:
                    if g not in growers:
                        to_remove.add((g, "harvestsBatch", batch))
                if not valid_growers and growers:
                    _add_relation((np.random.choice(growers), "harvestsBatch", batch))

            if to_remove:
                self._remove_triples(to_remove)

    def assign_most_specific(self):
        """
        Assigns the most specific class to each entity based on the hierarchy depth.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        hierarchy_depth = self.class_info["hierarchy_depth"] + 1
        shape = hierarchy_depth / (hierarchy_depth - 1)
        numbers = np.random.power(shape, size=len(self.is_typed))
        scaled_numbers = numbers / np.mean(numbers) * self.avg_depth_specific_class
        generated_numbers = np.clip(np.floor(scaled_numbers), 1, hierarchy_depth).astype(int)
        generated_numbers = [n if n < hierarchy_depth else hierarchy_depth - 1 for n in generated_numbers]
        self.current_avg_depth_specific_class = np.mean(generated_numbers)
        self.ent2layer_specific = {e: l for e, l in zip(self.is_typed, generated_numbers)}
        self.ent2classes_specific = {
            e: [np.random.choice(self.layer2classes[l])] for e, l in self.ent2layer_specific.items()
        }

    def complete_typing(self):
        """
        Completes the typing for the current entity (if multityping is enabled).

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        current_avg_multityping = 1.0
        entity_list = list(copy.deepcopy(self.is_typed))
        cpt = 0

        if entity_list:
            while current_avg_multityping < self.avg_multityping and cpt < 10:
                ent = np.random.choice(entity_list)
                most_specific_classes = self.ent2classes_specific[ent]
                specific_layer = self.ent2layer_specific[ent]
                compatible_classes = self.find_compatible_classes(most_specific_classes)
                specific_compatible_classes = list(
                    set(self.layer2classes[specific_layer]).intersection(set(compatible_classes))
                )
                specific_compatible_classes = [
                    cl for cl in specific_compatible_classes if cl not in most_specific_classes
                ]

                if specific_compatible_classes:
                    other_specific_class = np.random.choice(specific_compatible_classes)
                    self.ent2classes_specific[ent].append(other_specific_class)
                    current_avg_multityping = self.calculate_avg_multityping()
                    cpt = 0
                else:
                    cpt += 1

    def check_multityping(self):
        """
        Checks the multityping of entities and updates the badly typed entities.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        self.badly_typed = {}

        for e, classes in self.ent2classes_transitive.items():
            for c in classes:
                disj = self.class2disjoints_extended.get(c, [])
                if set(disj).intersection(classes):
                    self.badly_typed[e] = {"all_classes": classes, "problematic_class": c, "disjointwith": disj}
                    # keep only one of its most_specific classes and update its transitive classes
                    self.ent2classes_specific[e] = np.random.choice(self.ent2classes_specific[e])
                    self.ent2classes_transitive[e] = self.class_info["transitive_class2superclasses"][
                        self.ent2classes_specific[e]
                    ]
                    break

    def extend_superclasses(self):
        """
        Extends the superclasses of entities.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        self.ent2classes_transitive = {k: set(v) for k, v in self.ent2classes_specific.items()}

        for ent, specific_cls in self.ent2classes_specific.items():
            # Extend superclasses recursively
            for specific_cl in specific_cls:
                self.ent2classes_transitive[ent].update(
                    set(self.class_info["transitive_class2superclasses"][specific_cl])
                )
            self.ent2classes_transitive[ent] = list(set(self.ent2classes_transitive[ent]))

    def calculate_avg_multityping(self):
        """
        Calculates the average value of the multityping in the KG.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            float: The average value of multityping.
        """
        specific_cl_instanciations = len(list(itertools.chain(*self.ent2classes_specific.values())))
        return specific_cl_instanciations / len(self.is_typed)

    def find_compatible_classes(self, class_list):
        """
        Finds the classes that are compatible with the given class list.

        Args:
            self (object): The instance of the InstanceGenerator.
            class_list (list): A list of classes.

        Returns:
            set: A set of compatible classes.
        """
        disjoint_cls = set()

        for c in class_list:
            disjoint_cls.update(self.class2disjoints_extended.get(c, []))

        # Find all the classes that are not disjoint with any of the specific classes
        compatible_classes = [
            c
            for c in self.class2disjoints_extended.keys()
            if all(c not in self.class2disjoints_extended.get(specific_c, []) for specific_c in class_list)
        ]

        return set(compatible_classes) - set(class_list) | set(self.non_disjoint_classes)

    def assign_entity_labels(self):
        # Attach human-friendly labels to entities using Faker
        faker = self.faker
        if faker is None:
            try:
                from faker import Faker
                faker = Faker()
            except ImportError:
                faker = None
        labels = {}
        used = set()
        for ent in self.entities:
            classes = self.ent2classes_specific.get(ent, []) if hasattr(self, 'ent2classes_specific') else []
            base_label = self._generate_label_for_entity(ent, classes, faker)
            if not base_label:
                base_label = f"Entity {ent}"
            label = base_label
            counter = 2
            while label in used:
                label = f"{base_label} {counter}"
                counter += 1
            used.add(label)
            labels[ent] = label
        self.entity_labels = labels
        self.entity_name_map = self._build_entity_ids(labels)
        self._apply_entity_names()

    def _generate_label_for_entity(self, ent, classes, faker):
        if faker is None:
            return None
        prioritized = list(classes) if classes else []
        if "Organization" not in prioritized:
            prioritized.append("Organization")
        if "Entity" not in prioritized:
            prioritized.append("Entity")
        for class_name in prioritized:
            label = self._label_for_class(class_name, faker)
            if label:
                return label
        return None

    def _is_reserved_logistics_name(self, label):
        if not label:
            return False
        normalized = re.sub(r"[^a-z0-9]+", " ", label.lower()).strip()
        if not normalized:
            return False
        tokens = normalized.split()
        reserved_phrases = (
            ("fedex",),
            ("dhl",),
            ("ups",),
            ("maersk",),
            ("geodis",),
            ("ceva",),
            ("db", "schenker"),
            ("c", "h", "robinson"),
            ("ch", "robinson"),
            ("kuehne", "nagel"),
            ("xpo",),
        )
        for phrase in reserved_phrases:
            if len(phrase) == 1:
                if phrase[0] in tokens:
                    return True
            else:
                for idx in range(len(tokens) - len(phrase) + 1):
                    if tuple(tokens[idx : idx + len(phrase)]) == phrase:
                        return True
        return False

    def _label_for_class(self, class_name, faker):
        if faker is None:
            return None
        class_name = class_name or ""
        for _ in range(5):
            name = None
            if class_name == "Supplier":
                if hasattr(faker, "company_name_supplier"):
                    name = faker.company_name_supplier()
                else:
                    name = f"{faker.company()} Supplies"
            elif class_name == "Manufacturer":
                if hasattr(faker, "manufacturer_name"):
                    name = faker.manufacturer_name()
                else:
                    name = f"{faker.company()} Manufacturing"
            elif class_name == "Distributor":
                if hasattr(faker, "distributor_name"):
                    name = faker.distributor_name()
                else:
                    name = f"{faker.company()} Distribution"
            elif class_name == "CertificationBody":
                if hasattr(faker, "certification_body_name"):
                    name = faker.certification_body_name()
                else:
                    name = f"{faker.company()} Certification"
            elif class_name == "Regulator":
                if hasattr(faker, "regulator_name"):
                    name = faker.regulator_name()
                else:
                    name = f"{faker.city()} Food Authority"
            elif class_name == "Retailer":
                if hasattr(faker, "retailer_name"):
                    name = faker.retailer_name()
                else:
                    name = f"{faker.company()} Retail"
            elif class_name == "Grower":
                if hasattr(faker, "grower_name"):
                    name = faker.grower_name()
                else:
                    name = f"{faker.company()} Growers"
            elif class_name == "Product":
                if hasattr(faker, "product_name"):
                    name = faker.product_name()
                elif hasattr(faker, "catch_phrase"):
                    name = faker.catch_phrase()
                else:
                    name = faker.word().title()
            elif class_name == "Shipment":
                if hasattr(faker, "shipment_code"):
                    name = faker.shipment_code()
                else:
                    name = f"Shipment {faker.bothify('??-####').upper()}"
            elif class_name == "TransportVehicle":
                if hasattr(faker, "transport_vehicle_name"):
                    name = faker.transport_vehicle_name()
                else:
                    name = f"Truck {faker.bothify('??-####').upper()}"
            elif class_name == "Facility":
                if hasattr(faker, "facility_name"):
                    name = faker.facility_name()
                else:
                    suffix = faker.random_element(elements=("Plant", "Facility", "Hub", "Center", "Warehouse"))
                    name = f"{faker.city()} {suffix}"
            elif class_name == "Farm":
                if hasattr(faker, "farm_name"):
                    name = faker.farm_name()
                else:
                    name = f"{faker.last_name()} Farm"
            elif class_name == "Mill":
                if hasattr(faker, "mill_name"):
                    name = faker.mill_name()
                else:
                    name = f"{faker.city()} Mill"
            elif class_name == "Port":
                if hasattr(faker, "port_name"):
                    name = faker.port_name()
                else:
                    name = f"{faker.city()} Port"
            elif class_name == "ProcessingPlant":
                if hasattr(faker, "processing_plant_name"):
                    name = faker.processing_plant_name()
                else:
                    name = f"{faker.city()} Processing Plant"
            elif class_name == "QualityLab":
                if hasattr(faker, "quality_lab_name"):
                    name = faker.quality_lab_name()
                else:
                    name = f"{faker.city()} Quality Lab"
            elif class_name == "RetailStore":
                if hasattr(faker, "retail_store_name"):
                    name = faker.retail_store_name()
                else:
                    name = f"{faker.city()} Store"
            elif class_name == "Warehouse":
                if hasattr(faker, "warehouse_name"):
                    name = faker.warehouse_name()
                else:
                    name = f"{faker.city()} Warehouse"
            elif class_name == "DistributionCenter":
                if hasattr(faker, "distribution_center_name"):
                    name = faker.distribution_center_name()
                else:
                    name = f"{faker.city()} Distribution Center"
            elif class_name == "HarvestBatch":
                if hasattr(faker, "harvest_batch_label"):
                    name = faker.harvest_batch_label()
                else:
                    name = f"Harvest {faker.bothify('??-####').upper()}"
            elif class_name == "ProductBatch":
                if hasattr(faker, "product_batch_label"):
                    name = faker.product_batch_label()
                elif hasattr(faker, "harvest_batch_label"):
                    name = faker.harvest_batch_label()
                else:
                    name = f"ProductBatch {faker.bothify('??-####').upper()}"
            elif class_name == "MaterialLot":
                if hasattr(faker, "material_lot_label"):
                    name = faker.material_lot_label()
                elif hasattr(faker, "harvest_batch_label"):
                    name = faker.harvest_batch_label()
                else:
                    name = f"MaterialLot {faker.bothify('??-####').upper()}"
            elif class_name == "Location":
                name = f"{faker.city()}, {faker.country()}"
            elif class_name == "Organization":
                if hasattr(faker, "company_name_supplier"):
                    name = faker.company_name_supplier()
                else:
                    name = faker.company()
            elif class_name == "LogisticsProvider":
                if hasattr(faker, "logistics_provider_name"):
                    name = faker.logistics_provider_name()
                else:
                    name = faker.company()
            elif class_name == "Entity":
                name = None

            if name is None:
                return None
            if class_name != "LogisticsProvider" and self._is_reserved_logistics_name(name):
                continue
            return name
        return None

    def _build_entity_ids(self, labels):
        mapping = {}
        used = set()
        for original, label in labels.items():
            base = self._sanitize_label(label)
            candidate = base
            suffix = 2
            while candidate in used:
                candidate = f"{base}_{suffix}"
                suffix += 1
            used.add(candidate)
            mapping[original] = candidate
        return mapping

    def _sanitize_label(self, label):
        slug = re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_")
        if not slug:
            slug = "entity"
        if slug[0].isdigit():
            slug = f"entity_{slug}"
        return slug

    def _apply_entity_names(self):
        name_map = getattr(self, "entity_name_map", {})
        if not name_map:
            return
        self.entities = [name_map.get(ent, ent) for ent in self.entities]
        self.is_typed = {name_map.get(ent, ent) for ent in self.is_typed}
        if hasattr(self, "ent2classes_specific"):
            self.ent2classes_specific = {
                name_map.get(ent, ent): classes for ent, classes in self.ent2classes_specific.items()
            }
        if hasattr(self, "ent2classes_transitive"):
            self.ent2classes_transitive = {
                name_map.get(ent, ent): classes for ent, classes in self.ent2classes_transitive.items()
            }
        if hasattr(self, "ent2layer_specific"):
            self.ent2layer_specific = {
                name_map.get(ent, ent): layer for ent, layer in self.ent2layer_specific.items()
            }
        self.entity_labels = {name_map.get(ent, ent): label for ent, label in self.entity_labels.items()}

    def pipeline(self):
        """
        Pipeline for processing entities and subsequently generating triples.

        Args:
            self (object): The instance of the InstanceGenerator.

        Return:
            None
        """

        if self.fast_gen:
            self.entities = [f"E{i}" for i in range(1, int(self.num_entities / self.fast_ratio) + 1)]
        else:
            self.entities = [f"E{i}" for i in range(1, self.num_entities + 1)]

        entities = copy.deepcopy(self.entities)
        np.random.shuffle(entities)

        threshold = int(len(self.entities) * (1 - self.prop_untyped_entities))
        self.is_typed = set(entities[:threshold])

        self.layer2classes = {int(k): v for k, v in self.class_info["layer2classes"].items()}
        self.class2layer = self.class_info["class2layer"]
        self.class2disjoints_extended = self.class_info["class2disjoints_extended"]
        self.classes = self.class_info["classes"]
        self.non_disjoint_classes = set(self.classes) - set(self.class2disjoints_extended.keys())

        self.assign_most_specific()

        if self.multityping:
            self.complete_typing()

        self.extend_superclasses()
        self.check_multityping()

        if self.fast_gen:
            ent2classes_spec_values = list(self.ent2classes_specific.values())
            ent2classes_trans_values = list(self.ent2classes_transitive.values())
            last_ent = len(self.entities)

            for _ in range(1, self.fast_ratio):
                entity_batch = [
                    f"E{i}" for i in range(last_ent + 1, last_ent + int(self.num_entities / self.fast_ratio) + 1)
                ]
                np.random.shuffle(entity_batch)
                threshold = int(len(entity_batch) * (1 - self.prop_untyped_entities))
                typed_entities = entity_batch[:threshold]
                self.is_typed.update(typed_entities)
                ent2classes_specific = {e: ent2classes_spec_values[idx] for idx, e in enumerate(typed_entities)}
                ent2classes_transitive = {e: ent2classes_trans_values[idx] for idx, e in enumerate(typed_entities)}
                self.ent2classes_specific.update(ent2classes_specific)
                self.ent2classes_transitive.update(ent2classes_transitive)
                self.entities += entity_batch
                last_ent = len(self.entities)

        self.assign_entity_labels()

        self.generate_triples()

    def distribute_relations(self):
        """
        Distributes relations based on the number of triples and the relation balance ratio.

        Args:
            self (object): The instance of the InstanceGenerator.

        Return:
            None

        """
        self.num_relations = len(self.relation_info["relations"])

        if self.num_triples < self.num_relations:
            self.triples_per_rel = {f"R{i}": 1 if i < self.num_triples else 0 for i in range(self.num_relations)}
            self.relation_weights = np.ones(self.num_relations, dtype=float) / self.num_relations
        else:
            self.relation_weights = self._compute_relation_weights()
            self.triples_per_rel = {
                r: np.ceil(tpr)
                for r, tpr in zip(self.relation_info["relations"], np.array(self.relation_weights) * self.num_triples)
            }

    def _compute_relation_weights(self):
        """
        Builds relation sampling weights, optionally using power-law style distributions.
        """
        relations = self.relation_info["relations"]
        size = len(relations)
        dist_cfg = self.relation_distribution if isinstance(self.relation_distribution, dict) else {}
        dist_type = (dist_cfg.get("type") or "").lower()

        samples = None
        if dist_type in {"pareto", "power_law"}:
            alpha = max(float(dist_cfg.get("alpha", 1.5)), np.finfo(float).tiny)
            samples = np.random.pareto(alpha, size) + 1.0
        elif dist_type == "zipf":
            alpha = max(float(dist_cfg.get("alpha", 2.0)), 1.001)
            samples = np.random.zipf(alpha, size).astype(float)
        elif dist_type == "custom":
            weights = dist_cfg.get("weights", [])
            if isinstance(weights, list) and len(weights) == size:
                samples = np.array(weights, dtype=float)

        if samples is None:
            mean = int(self.num_triples / size)
            spread_coeff = (1 - self.relation_balance_ratio) * mean
            return generate_random_numbers(mean, spread_coeff, size)

        samples = np.array(samples, dtype=float)
        samples = np.clip(samples, np.finfo(float).tiny, None)
        weights = samples / samples.sum()
        weights = self._apply_relation_hotspots(weights, dist_cfg)
        return weights

    def _apply_relation_hotspots(self, weights, dist_cfg):
        """
        Applies optional per-relation multipliers before renormalising.
        """
        hotspots = dist_cfg.get("hotspots", []) if isinstance(dist_cfg, dict) else []
        if not hotspots:
            return weights

        weight_arr = np.array(weights, dtype=float)
        rel_index = {rel: idx for idx, rel in enumerate(self.relation_info["relations"])}
        for hotspot in hotspots:
            if not isinstance(hotspot, dict):
                continue
            rel = hotspot.get("relation")
            if rel not in rel_index:
                continue
            multiplier = hotspot.get("multiplier", 1.0)
            try:
                multiplier = float(multiplier)
            except (TypeError, ValueError):
                continue
            if multiplier <= 0:
                continue
            weight_arr[rel_index[rel]] *= multiplier

        total = weight_arr.sum()
        if total <= 0:
            return np.ones_like(weight_arr) / len(weight_arr)
        return weight_arr / total

    def generate_triples(self):
        """
        Generates triples for the KG.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        self.class2entities = {}

        for e, classes in self.ent2classes_transitive.items():
            for c in classes:
                self.class2entities.setdefault(c, []).append(e)

        self._init_entity_popularity()

        self.class2unseen = copy.deepcopy(self.class2entities)
        self.flattened_unseen = list(set((itertools.chain(*self.class2entities.values()))))

        self.untyped_entities_priority = set(self.entities) - set(self.is_typed)
        self.untyped_entities = list(copy.deepcopy(self.untyped_entities_priority))

        self.rel2dom = self.relation_info["rel2dom"]
        self.rel2range = self.relation_info["rel2range"]
        self.rel2patterns = self.relation_info["rel2patterns"]
        self.rel2inverse = self.relation_info["rel2inverse"]
        self.functional_usage = {rel: {} for rel in self.functional_relations}
        self.inversefunctional_usage = {rel: {} for rel in self.inversefunctional_relations}

        self.kg = set()

        self.distribute_relations()

        self.last_oversample = 0

        attempt = 0
        max_attempts = 100
        while len(self.kg) < self.num_triples:
            rnd_r = np.random.choice(self.relation_info["relations"], p=self.relation_weights)
            new_triple = self.generate_one_triple(rnd_r)
            if None in new_triple:
                attempt += 1
                if attempt > max_attempts:
                    break
                continue
            if self.check_consistency(new_triple):
                if new_triple not in self.kg:
                    self.kg.add(new_triple)
                self._register_functional_usage(*new_triple)
                attempt = 0
            else:
                attempt += 1
                if attempt > max_attempts:
                    break

    def _init_entity_popularity(self):
        self.class2entity_weights = {}
        self.class2entity_index = {}
        if not isinstance(self.popularity_skew, dict):
            self.popularity_skew = {}

        for cls, entities in self.class2entities.items():
            self.class2entity_index[cls] = {entity: idx for idx, entity in enumerate(entities)}
            self.class2entity_weights[cls] = self._build_weight_vector_for_class(cls, len(entities))

    def _resolve_class_popularity_config(self, class_name):
        cfg = self.popularity_skew if isinstance(self.popularity_skew, dict) else {}
        config = {}
        default_cfg = cfg.get("default")
        if isinstance(default_cfg, dict):
            config.update(default_cfg)

        for section_key in ("overrides", "class_overrides"):
            section = cfg.get(section_key)
            if isinstance(section, dict):
                override = section.get(class_name)
                if isinstance(override, dict):
                    config.update(override)

        direct_override = cfg.get(class_name)
        if isinstance(direct_override, dict):
            config.update(direct_override)

        return config

    def _build_weight_vector_for_class(self, class_name, size):
        if size == 0:
            return np.array([], dtype=float)

        config = self._resolve_class_popularity_config(class_name)
        distribution = (config.get("distribution") or "uniform").lower()

        samples = None
        if distribution in {"pareto", "power_law"}:
            alpha = max(float(config.get("alpha", 1.5)), np.finfo(float).tiny)
            samples = np.random.pareto(alpha, size) + 1.0
        elif distribution == "zipf":
            alpha = max(float(config.get("alpha", 2.0)), 1.001)
            samples = np.random.zipf(alpha, size).astype(float)
        elif distribution == "custom":
            weights = config.get("weights", [])
            if isinstance(weights, list) and len(weights) == size:
                samples = np.array(weights, dtype=float)

        if samples is None:
            samples = np.ones(size, dtype=float)

        head_share = config.get("head_share")
        head_boost = config.get("head_boost", 1.0)
        if head_share and head_boost and head_boost > 1.0:
            if isinstance(head_share, (int, float)):
                if head_share < 1:
                    head_count = max(1, int(size * head_share))
                else:
                    head_count = min(size, int(head_share))
            else:
                head_count = 0
            if head_count > 0:
                top_indices = np.argsort(samples)[-head_count:]
                samples[top_indices] *= head_boost

        min_weight = config.get("min_weight")
        if min_weight is not None:
            try:
                min_weight = float(min_weight)
            except (TypeError, ValueError):
                min_weight = None
        if min_weight:
            samples = np.maximum(samples, min_weight)

        total = samples.sum()
        if total <= 0:
            return np.ones(size, dtype=float) / size
        return samples / total

    def _sample_unseen_entity(self, class_name):
        unseen = self.class2unseen.get(class_name, [])
        if not unseen:
            return None

        weights = self.class2entity_weights.get(class_name)
        indices_map = self.class2entity_index.get(class_name, {})
        if weights is None or not indices_map:
            return np.random.choice(unseen)

        indices = [indices_map.get(entity) for entity in unseen]
        indices = [idx for idx in indices if idx is not None]
        if not indices:
            return np.random.choice(unseen)

        subset_weights = weights[indices]
        total = subset_weights.sum()
        if total <= 0:
            return np.random.choice(unseen)
        subset_weights = subset_weights / total
        population = [self.class2entities[class_name][idx] for idx in indices]
        return np.random.choice(population, p=subset_weights)

    def _sample_entity(self, class_name):
        entities = self.class2entities.get(class_name, [])
        if not entities:
            return None

        weights = self.class2entity_weights.get(class_name)
        if weights is not None and len(weights) == len(entities):
            return np.random.choice(entities, p=weights)
        return np.random.choice(entities)

    def _assign_dataproperty_values(self):
        dataproperties = self.dataproperty_info.get("dataproperties", []) if hasattr(self, "dataproperty_info") else []
        self.literal_values_raw = {}
        self._code_sequences = {}
        if not dataproperties:
            return

        priorities = self._dataproperty_priority()
        ordered_props = sorted(dataproperties, key=lambda dp: priorities.get(dp, 1000))

        for prop in ordered_props:
            domain = self.dp2dom.get(prop)
            datatype = self.dp2datatype.get(prop)
            if not domain:
                continue
            entities = self.class2entities.get(domain, [])
            if not entities:
                continue
            config = self.value_profiles.get(prop, {})

            for entity in entities:
                generated = self._generate_value_for_property(entity, prop, datatype, config)
                if generated is None:
                    continue
                if not isinstance(generated, list):
                    values = [generated]
                else:
                    values = [value for value in generated if value is not None]
                if not values:
                    continue
                store = self.literal_values_raw.setdefault(entity, {})
                if prop in self.functional_dataproperties:
                    store[prop] = [values[0]]
                else:
                    store.setdefault(prop, [])
                    store[prop].extend(values)

    def _dataproperty_priority(self):
        default_priority = {
            "harvestDate": 10,
            "harvestWeightKg": 11,
            "cropType": 12,
            "batchCode": 20,
            "shipmentDate": 30,
            "shipmentId": 31,
            "shipmentStatus": 32,
            "organizationName": 40,
        }
        custom_priority = self.dataproperty_priority if isinstance(self.dataproperty_priority, dict) else {}
        merged = default_priority.copy()
        merged.update(custom_priority)
        return merged

    def _generate_value_for_property(self, entity, prop, datatype, config):
        config = config or {}
        generator_type = (config.get("type") or "").lower()

        if not generator_type:
            if any(key in config for key in ("mean", "stddev")):
                generator_type = "truncated_normal"
            elif any(key in config for key in ("values", "choices")):
                generator_type = "choice"
            elif datatype == "xsd:boolean":
                generator_type = "boolean"

        if prop == "organizationName":
            classes = self.ent2classes_transitive.get(entity, []) if hasattr(self, "ent2classes_transitive") else []
            if "LogisticsProvider" in classes or "LogisticsProvider" in self.ent2classes_specific.get(entity, []):
                label = getattr(self, "entity_labels", {}).get(entity)
                if label:
                    return label

        # Direct property-specific generators (override defaults)
        if prop == "harvestDate":
            return self._generate_harvest_datetime(entity)
        if prop == "shipmentDate":
            return self._generate_shipment_datetime(entity)
        if prop == "cropType":
            return self._sample_crop_type()
        if prop == "shipmentStatus":
            return self._sample_shipment_status()

        if generator_type == "truncated_normal":
            mean = float(config.get("mean", 0))
            std = float(config.get("stddev", 1))
            min_val = config.get("min")
            max_val = config.get("max")
            value = self._sample_truncated_normal(mean, std, min_val, max_val)
            decimals = config.get("decimals")
            if decimals is not None and value is not None:
                value = round(float(value), decimals)
            return value
        if generator_type == "uniform":
            min_val = config.get("min")
            max_val = config.get("max")
            value = self._sample_uniform(min_val, max_val)
            decimals = config.get("decimals")
            if decimals is not None and value is not None:
                value = round(float(value), decimals)
            return value
        if generator_type == "date_range":
            return self._strip_microseconds(self._sample_date_range(config))
        if generator_type == "datetime_range":
            return self._strip_microseconds(self._sample_datetime_range(config))
        if generator_type == "relation_offset":
            return self._strip_microseconds(self._generate_relation_offset(entity, config))
        if generator_type == "property_offset":
            return self._strip_microseconds(self._generate_property_offset(entity, config))
        if generator_type == "ratio":
            return self._generate_ratio_value(entity, config)
        if generator_type == "code":
            return self._generate_code_value(entity, prop, config)
        if generator_type == "choice":
            choices = config.get("values") or config.get("choices") or []
            if not choices:
                return None
            weights = config.get("weights")
            if weights and len(weights) == len(choices):
                return random.choices(choices, weights=weights, k=1)[0]
            return random.choice(choices)
        if generator_type == "boolean":
            probability = float(config.get("probability", 0.5))
            return np.random.random() < probability
        if generator_type == "text":
            return self._generate_text_value(config)
        if generator_type == "faker":
            method = config.get("method")
            if self.faker and method and hasattr(self.faker, method):
                faker_method = getattr(self.faker, method)
                try:
                    return faker_method()
                except TypeError:
                    return None
            return None
        if generator_type == "blend_ratio":
            return self._generate_blend_description(config)

        return self._default_value_for_datatype(datatype, prop)

    def _generate_harvest_datetime(self, entity):
        """Generate a realistic harvest datetime; reuse if already generated."""
        classes = self.ent2classes_transitive.get(entity, []) if hasattr(self, "ent2classes_transitive") else []
        if "HarvestBatch" not in classes:
            return None
        existing = self._get_first_raw_value(entity, "harvestDate")
        if existing:
            return existing
        today = dt.datetime.utcnow()
        start_days = 30
        end_days = 160
        days_ago = random.randint(start_days, end_days)
        base = today - dt.timedelta(days=days_ago)
        hour = random.randint(5, 18)
        minute = random.randint(0, 59)
        harvest_dt = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return harvest_dt

    def _generate_shipment_datetime(self, shipment_ent):
        """Generate shipmentDate strictly after related harvestDate values."""
        related_batches = set(self._get_related_entities(shipment_ent, "carriesBatch"))
        related_batches.update(self._get_related_entities(shipment_ent, "includedInShipment", inverse=True))

        harvest_dates = []
        for batch in related_batches:
            hd = self._generate_harvest_datetime(batch)
            if hd:
                # ensure the raw store is populated for later access
                self.literal_values_raw.setdefault(batch, {}).setdefault("harvestDate", [hd])
                harvest_dates.append(hd)

        if harvest_dates:
            base = max(harvest_dates)
        else:
            # Fallback: create a harvest baseline and store it for a synthetic batch id
            base = dt.datetime.utcnow() - dt.timedelta(days=random.randint(7, 60))

        delta_days = random.randint(1, 14)
        delta_hours = random.randint(2, 18)
        shipment_dt = base + dt.timedelta(days=delta_days, hours=delta_hours)
        return shipment_dt.replace(microsecond=0)

    def _sample_crop_type(self):
        crops = ["Olives", "Coffee", "Wheat", "Corn", "Soy"]
        return random.choice(crops)

    def _sample_shipment_status(self):
        return random.choice(["In-Transit", "Delivered", "Pending", "Delayed"])

    def _generate_relation_offset(self, entity, config):
        relation = config.get("relation")
        reference_property = config.get("reference_property")
        if not relation or not reference_property:
            return None
        inverse = (config.get("direction") or "forward").lower() == "inverse"
        related_entities = self._get_related_entities(entity, relation, inverse=inverse)

        base_values = []
        for related in related_entities:
            base_value = self._get_first_raw_value(related, reference_property)
            base_dt = self._parse_datetime(base_value)
            if base_dt:
                base_values.append(base_dt)

        if not base_values:
            return self._strip_microseconds(self._sample_datetime_range(config))

        base = max(base_values)
        delta = self._build_timedelta(config)
        if delta is None:
            delta = dt.timedelta(hours=12)
        return self._strip_microseconds(base + delta)

    def _generate_property_offset(self, entity, config):
        reference_property = config.get("reference_property")
        if not reference_property:
            return None
        base_value = self._get_first_raw_value(entity, reference_property)
        base_dt = self._parse_datetime(base_value)
        if not base_dt:
            return self._strip_microseconds(self._sample_datetime_range(config))
        delta = self._build_timedelta(config)
        if delta is None:
            delta = dt.timedelta(days=1)
        return self._strip_microseconds(base_dt + delta)

    def _generate_ratio_value(self, entity, config):
        ratio_min = float(config.get("ratio_min", 0))
        ratio_max = float(config.get("ratio_max", ratio_min))
        if ratio_max < ratio_min:
            ratio_max = ratio_min

        base_values = []
        relation = config.get("source_relation")
        source_property = config.get("source_property")
        if relation and source_property:
            for related in self._get_related_entities(entity, relation):
                value = self._coerce_numeric(self._get_first_raw_value(related, source_property))
                if value is not None:
                    base_values.append(value)

        local_property = config.get("local_property")
        if local_property:
            value = self._coerce_numeric(self._get_first_raw_value(entity, local_property))
            if value is not None:
                base_values.append(value)

        if not base_values:
            base = self._coerce_numeric(config.get("fallback_base"))
            if base is None:
                return None
        else:
            base = max(base_values)

        ratio = self._random_between(ratio_min, ratio_max)
        if ratio is None:
            return None
        value = base * ratio
        decimals = config.get("decimals")
        if decimals is not None:
            value = round(float(value), decimals)
        return value

    def _generate_code_value(self, entity, prop, config):
        prefix = config.get("prefix", "")
        digits = int(config.get("digits", 4))
        separator = config.get("separator", "-")
        start = int(config.get("start", 1))
        seq = self._code_sequences.get(prop, start)
        self._code_sequences[prop] = seq + 1

        year = None
        year_property = config.get("year_property")
        if year_property:
            year_value = self._get_first_raw_value(entity, year_property)
            year_dt = self._parse_datetime(year_value)
            if year_dt:
                year = year_dt.year

        pattern = config.get("pattern")
        seq_str = str(seq).zfill(digits)
        if pattern:
            return pattern.format(prefix=prefix, seq=seq_str, year=year or "", entity=entity)

        parts = []
        if prefix:
            parts.append(prefix)
        if year:
            parts.append(str(year))
        parts.append(seq_str)
        return separator.join(parts) if separator else "".join(parts)

    def _generate_blend_description(self, config):
        cultivars = config.get("cultivars") or ["Arbequina", "Koroneiki", "Picual"]
        if not cultivars:
            cultivars = ["Arbequina", "Koroneiki"]
        min_components = int(config.get("min_components", 2))
        max_components = int(config.get("max_components", 3))
        max_components = min(max_components, len(cultivars))
        if max_components <= 0:
            max_components = 1
        count = random.randint(max(1, min_components), max_components)
        selected = random.sample(cultivars, count)

        splits = sorted(random.sample(range(1, 100), count - 1))
        percentages = []
        prev = 0
        for split in splits + [100]:
            percentages.append(split - prev)
            prev = split
        return ", ".join(f"{pct}% {cultivar}" for pct, cultivar in zip(percentages, selected))

    def _generate_text_value(self, config):
        sentences = int(config.get("sentences", 1))
        words = int(config.get("words", 12))
        faker = self.faker
        if faker:
            return " ".join(faker.sentence(nb_words=words).strip() for _ in range(sentences)).strip()
        vocabulary = [
            "fruity",
            "balanced",
            "peppery",
            "herbal",
            "aromatic",
            "smooth",
            "vibrant",
            "nuanced",
            "fresh",
            "citrus",
            "almond",
            "complex",
        ]
        words_list = [random.choice(vocabulary) for _ in range(words * sentences)]
        return " ".join(words_list)

    def _default_value_for_datatype(self, datatype, prop):
        dtype = (datatype or "").lower()
        faker = self.faker
        if dtype == "xsd:boolean":
            return bool(random.getrandbits(1))
        if dtype == "xsd:decimal":
            return round(float(np.random.uniform(0, 1)), 2)
        if dtype == "xsd:dateTime":
            return dt.datetime.utcnow().replace(microsecond=0)
        if dtype == "xsd:string":
            if faker:
                return faker.bothify(f"{prop[:3].upper()}-####")
            return f"{prop}_{np.random.randint(1_000, 9_999)}"
        return None

    def _generate_geo_coordinates(self):
        """Generate geo:lat/geo:long for every Farm and Mill."""
        coords = {}
        
        land_boxes = [
            (37.0, 43.5, -7.5, 3.0),   
            (41.5, 46.5, -1.0, 6.0),   
            (38.0, 45.0, 7.0, 14.5),   
            (36.5, 41.0, 14.0, 20.0),  
            (37.0, 41.5, 20.0, 24.5),  
        ]

        def sample_land_point():
            box = random.choice(land_boxes)
            lat = np.random.uniform(box[0], box[1])
            lon = np.random.uniform(box[2], box[3])
            return round(lat, 5), round(lon, 5)

        for cls in ("Farm", "Mill"):
            for ent in self.class2entities.get(cls, []):
                coords[ent] = sample_land_point()
        return coords

    def _group_shipments_by_route(self):
        """
        Rebuild shipments so each unique (from, to, date) route has exactly one shipment
        carrying all batches on that route, with a single origin/destination and a shippedBy logistics provider.
        """
        if "Shipment" not in self.class_info.get("classes", []):
            return
        if "LogisticsProvider" not in self.class_info.get("classes", []):
            return
        if "ProductBatch" in self.class_info.get("classes", []):
            return
        shipments = set(self.class2entities.get("Shipment", []))
        if not shipments:
            return

        rel_head, rel_tail, rel2triples = self._build_relation_index()
        def _descendants(cls_name):
            subs = self.class_info.get("transitive_class2subclasses", {}).get(cls_name, [])
            return set(self.class2entities.get(cls_name, []) + list(itertools.chain(*(self.class2entities.get(c, []) for c in subs if c in self.class2entities))))

        facility_classes = _descendants("Facility")
        mills_only = _descendants("Mill")
        warehouses = _descendants("Warehouse")
        dist_centers = _descendants("DistributionCenter")
        allowed_dests = set().union(mills_only, warehouses, dist_centers) or facility_classes
        harvest_batches = set(self.class2entities.get("HarvestBatch", []))
        logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            self._ensure_logistics_providers()
            logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            raise RuntimeError("No LogisticsProvider entities available for shippedBy assignment.")

        def get_shipment_date(sh):
            vals = self.literal_values_raw.get(sh, {}).get("shipmentDate", [])
            if not vals:
                return None
            v = vals[0]
            dt_obj = self._parse_datetime(v)
            return dt_obj or v

        def batch_origin(batch):
            # prefer processedAt then harvestedFrom
            locs = rel_head.get((batch, "processedAt"))
            if locs:
                return locs[0]
            locs = rel_head.get((batch, "harvestedFrom"))
            return locs[0] if locs else None

        def batch_dest(batch):
            dests = rel_head.get((batch, "shippedTo"))
            if dests:
                return dests[0]
            carriers = rel_tail.get((batch, "carriesBatch"), [])
            for sh in carriers:
                to_vals = rel_head.get((sh, "shippedTo"), [])
                if to_vals:
                    return to_vals[0]
            return None

        # Collect batches and their existing routes/dates
        batch_routes = {}
        for sh, batch in [(h, t) for h, t in rel2triples.get("carriesBatch", []) if h in shipments]:
            from_vals = rel_head.get((sh, "shippedFrom"), [])
            to_vals = rel_head.get((sh, "shippedTo"), [])
            src = from_vals[0] if from_vals else None
            dst = to_vals[0] if to_vals else None
            date = get_shipment_date(sh)
            if src and dst and src != dst:
                batch_routes.setdefault(batch, []).append((src, dst, date))

        # Build grouping key per batch
        batches_by_route = defaultdict(list)
        for batch, routes in batch_routes.items():
            src = None
            dst = None
            date = None
            if routes:
                src = routes[0][0]
                dst = routes[0][1]
                date = routes[0][2]
            if not src:
                src = batch_origin(batch)
            if not dst:
                dst = batch_dest(batch)
            if not src or src not in facility_classes:
                continue  # require real facility origin
            # Teleportation guard: origin must match batch's current location
            batch_loc = batch_origin(batch)
            if batch_loc and src != batch_loc:
                continue
            # Type safety: dst must be allowed destination class
            if dst not in allowed_dests:
                continue
            # Ensure shippedTo is not a batch
            if dst in harvest_batches:
                continue
            if src == dst:
                continue
            key = (src, dst, str(date) if date else None)
            batches_by_route[key].append(batch)

        if not batches_by_route:
            return

        # Remove all existing shipment triples and shipment entities
        triples_to_remove = {(h, r, t) for h, r, t in self.kg if h in shipments or t in shipments}
        if triples_to_remove:
            self._remove_triples(triples_to_remove)

        # drop shipment entries from bookkeeping
        self.entities = [e for e in self.entities if e not in shipments]
        self.is_typed = {e for e in self.is_typed if e not in shipments}
        if "Shipment" in self.class2entities:
            self.class2entities["Shipment"] = []
        for ent in shipments:
            self.ent2classes_specific.pop(ent, None)
            self.ent2classes_transitive.pop(ent, None)
            self.literal_values_raw.pop(ent, None)

        logistics_pool = [
            lp
            for lp in logistics_pool
        ]
        facilities_all = (
            self.class2entities.get("Facility", [])
            + self.class2entities.get("Farm", [])
            + self.class2entities.get("Mill", [])
        )

        new_shipments = []
        for idx, ((src, dst, date), batches) in enumerate(batches_by_route.items(), start=1):
            sh_id = f"Shipment_RT_{idx}"
            new_shipments.append(sh_id)
            self.entities.append(sh_id)
            self.is_typed.add(sh_id)
            self.ent2classes_specific[sh_id] = ["Shipment"]
            supers = self.class_info["transitive_class2superclasses"].get("Shipment", [])
            self.ent2classes_transitive[sh_id] = list(set(["Shipment"] + list(supers)))
            self.class2entities.setdefault("Shipment", []).append(sh_id)

            # shippedFrom / shippedTo
            for rel, target in (("shippedFrom", src), ("shippedTo", dst)):
                if target and target in facilities_all and self.check_consistency((sh_id, rel, target)):
                    self.kg.add((sh_id, rel, target))
                    self._register_functional_usage(sh_id, rel, target)

            # shippedBy
            if logistics_pool:
                carrier = np.random.choice(logistics_pool)
                if self.check_consistency((sh_id, "shippedBy", carrier)):
                    self.kg.add((sh_id, "shippedBy", carrier))
                    self._register_functional_usage(sh_id, "shippedBy", carrier)

            # carries batches
            for batch in set(batches):
                if self.check_consistency((sh_id, "carriesBatch", batch)):
                    self.kg.add((sh_id, "carriesBatch", batch))
                    self._register_functional_usage(sh_id, "carriesBatch", batch)

            # shipmentDate literal
            if date:
                parsed = self._parse_datetime(date) or date
                self.literal_values_raw.setdefault(sh_id, {})["shipmentDate"] = [parsed]

    def _seed_batch_shipments(self):
        """For each processed batch, create a shipment using its current location as origin (flush all)."""
        classes = set(self.class_info.get("classes", []))
        required_classes = {"Shipment", "LogisticsProvider"}
        if not required_classes.issubset(classes):
            return
        if "ProductBatch" in classes:
            self._seed_product_batch_shipments()
            return
        if "HarvestBatch" not in classes:
            return
        if not hasattr(self, "literal_values_raw"):
            self.literal_values_raw = {}
        rel_head, rel_tail, rel2triples = self._build_relation_index()

        facilities = set(self.class2entities.get("Facility", []))
        facilities.update(self.class2entities.get("Farm", []))
        facilities.update(self.class2entities.get("Mill", []))

        mills_only = set(self.class2entities.get("Mill", []))
        warehouses = set(self.class2entities.get("Warehouse", []))
        dist_centers = set(self.class2entities.get("DistributionCenter", []))
        allowed_dests = mills_only | warehouses | dist_centers or mills_only or facilities

        logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            self._ensure_logistics_providers()
            logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            raise RuntimeError("No LogisticsProvider entities available for shippedBy assignment.")

        def batch_origin(batch):
            locs = rel_head.get((batch, "processedAt"))
            if locs:
                return locs[0]
            locs = rel_head.get((batch, "harvestedFrom"))
            return locs[0] if locs else None

        processed_batches = [b for _, b in rel2triples.get("processedAt", [])]
        if not processed_batches:
            return

        next_id = len(self.entities) + 1
        for batch in processed_batches:
            origin = batch_origin(batch)
            if origin is None or origin not in facilities:
                continue

            dest_choices = [d for d in allowed_dests if d != origin]
            if not dest_choices:
                continue
            dest = np.random.choice(dest_choices)

            sh_id = f"Shipment_PR_{next_id}"
            next_id += 1
            while sh_id in self.entities:
                sh_id = f"Shipment_PR_{next_id}"
                next_id += 1

            # Register entity typing
            self.entities.append(sh_id)
            self.is_typed.add(sh_id)
            self.ent2classes_specific[sh_id] = ["Shipment"]
            supers = self.class_info["transitive_class2superclasses"].get("Shipment", [])
            self.ent2classes_transitive[sh_id] = list(set(["Shipment"] + list(supers)))
            self.class2entities.setdefault("Shipment", []).append(sh_id)

            # shippedFrom / shippedTo
            for rel, target in (("shippedFrom", origin), ("shippedTo", dest)):
                if self.check_consistency((sh_id, rel, target)):
                    self.kg.add((sh_id, rel, target))
                    self._register_functional_usage(sh_id, rel, target)

            # shippedBy from LogisticsProvider only
            carrier = np.random.choice(logistics_pool)
            if self.check_consistency((sh_id, "shippedBy", carrier)):
                self.kg.add((sh_id, "shippedBy", carrier))
                self._register_functional_usage(sh_id, "shippedBy", carrier)

            # carriesBatch
            if self.check_consistency((sh_id, "carriesBatch", batch)):
                self.kg.add((sh_id, "carriesBatch", batch))
                self._register_functional_usage(sh_id, "carriesBatch", batch)

            # shipmentDate: harvestDate + 2-5 days as JIT logistics
            base_dt = None
            if batch in self.literal_values_raw:
                hd = self.literal_values_raw.get(batch, {}).get("harvestDate", [])
                if hd:
                    base_dt = self._parse_datetime(hd[0])
            if base_dt is None:
                base_dt = dt.datetime.utcnow()
            delta_days = random.randint(2, 5)
            ship_dt = base_dt + dt.timedelta(days=delta_days)
            self.literal_values_raw.setdefault(sh_id, {})["shipmentDate"] = [self._strip_microseconds(ship_dt)]

    def _seed_product_batch_shipments(self):
        """Create outbound shipments for ProductBatch entities from mills/plants."""
        if not hasattr(self, "literal_values_raw"):
            self.literal_values_raw = {}
        rel_head, rel_tail, rel2triples = self._build_relation_index()

        facility_classes = set(self.class_info.get("transitive_class2subclasses", {}).get("Facility", []))
        facility_classes.add("Facility")
        facilities = set()
        for cls in facility_classes:
            facilities.update(self.class2entities.get(cls, []))

        mills_only = set(self.class2entities.get("Mill", []))
        plants = set(self.class2entities.get("ProcessingPlant", []))
        warehouses = set(self.class2entities.get("Warehouse", []))
        dist_centers = set(self.class2entities.get("DistributionCenter", []))
        retail_stores = set(self.class2entities.get("RetailStore", []))
        ports = set(self.class2entities.get("Port", []))

        preferred_dests = dist_centers | warehouses | retail_stores | ports
        allowed_dests = preferred_dests or facilities

        logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            self._ensure_logistics_providers()
            logistics_pool = list(dict.fromkeys(self.class2entities.get("LogisticsProvider", [])))
        if not logistics_pool:
            raise RuntimeError("No LogisticsProvider entities available for shippedBy assignment.")

        product_batches = list(dict.fromkeys(self.class2entities.get("ProductBatch", [])))
        if not product_batches:
            return
        harvest_batches = set(self.class2entities.get("HarvestBatch", []))

        shipments = set(self.class2entities.get("Shipment", []))
        if shipments and harvest_batches:
            to_remove = set()
            for sh, batch in rel2triples.get("carriesBatch", []):
                if sh not in shipments or batch not in harvest_batches:
                    continue

                origins = rel_head.get((sh, "shippedFrom"), [])
                dests = rel_head.get((sh, "shippedTo"), [])
                for origin in origins:
                    if origin in mills_only or origin in plants:
                        to_remove.add((sh, "shippedFrom", origin))
                for dest in dests:
                    if dest not in mills_only and dest not in plants:
                        to_remove.add((sh, "shippedTo", dest))

                remaining_origins = [o for o in origins if (sh, "shippedFrom", o) not in to_remove]
                remaining_dests = [d for d in dests if (sh, "shippedTo", d) not in to_remove]
                if not remaining_origins or not remaining_dests:
                    carries_pb = any(
                        b in product_batches
                        for b in rel_head.get((sh, "carriesBatch"), [])
                    )
                    if carries_pb:
                        to_remove.add((sh, "carriesBatch", batch))
                        if (batch, "includedInShipment", sh) in self.kg:
                            to_remove.add((batch, "includedInShipment", sh))
                    else:
                        for h, r, t in list(self.kg):
                            if h == sh or t == sh:
                                to_remove.add((h, r, t))
            if to_remove:
                self._remove_triples(to_remove)

        def origin_for_batch(batch):
            plants_local = rel_tail.get((batch, "packagesBatch"), []) if "packagesBatch" in self.rel2dom else []
            if plants_local:
                return plants_local[0]
            derived = rel_head.get((batch, "batchDerivedFrom"), []) if "batchDerivedFrom" in self.rel2dom else []
            for hb in derived:
                locs = rel_head.get((hb, "processedAt"), [])
                if locs:
                    return locs[0]
            return None

        def shipment_date_for_batch(batch):
            derived = rel_head.get((batch, "batchDerivedFrom"), []) if "batchDerivedFrom" in self.rel2dom else []
            if derived:
                hb = derived[0]
                existing = getattr(self, "literal_values_raw", {}).get(hb, {}).get("harvestDate", [])
                if existing:
                    base_dt = self._parse_datetime(existing[0])
                else:
                    base_dt = dt.datetime.utcnow()
            else:
                base_dt = dt.datetime.utcnow()
            delta_days = random.randint(1, 7)
            delta_hours = random.randint(1, 12)
            return self._strip_microseconds(base_dt + dt.timedelta(days=delta_days, hours=delta_hours))

        next_id = len(self.entities) + 1
        for batch in product_batches:
            origin = origin_for_batch(batch)
            if origin is None or origin not in facilities:
                continue

            dest_choices = [d for d in allowed_dests if d != origin and d not in mills_only and d not in plants]
            if not dest_choices:
                continue
            dest = np.random.choice(dest_choices)

            sh_id = f"Shipment_PRD_{next_id}"
            next_id += 1
            while sh_id in self.entities:
                sh_id = f"Shipment_PRD_{next_id}"
                next_id += 1

            self.entities.append(sh_id)
            self.is_typed.add(sh_id)
            self.ent2classes_specific[sh_id] = ["Shipment"]
            supers = self.class_info["transitive_class2superclasses"].get("Shipment", [])
            self.ent2classes_transitive[sh_id] = list(set(["Shipment"] + list(supers)))
            self.class2entities.setdefault("Shipment", []).append(sh_id)

            for rel, target in (("shippedFrom", origin), ("shippedTo", dest)):
                if self.check_consistency((sh_id, rel, target)):
                    self.kg.add((sh_id, rel, target))
                    self._register_functional_usage(sh_id, rel, target)

            carrier = np.random.choice(logistics_pool)
            if self.check_consistency((sh_id, "shippedBy", carrier)):
                self.kg.add((sh_id, "shippedBy", carrier))
                self._register_functional_usage(sh_id, "shippedBy", carrier)

            if self.check_consistency((sh_id, "carriesBatch", batch)):
                self.kg.add((sh_id, "carriesBatch", batch))
                self._register_functional_usage(sh_id, "carriesBatch", batch)

            ship_dt = shipment_date_for_batch(batch)
            self.literal_values_raw.setdefault(sh_id, {})["shipmentDate"] = [ship_dt]

    def _generate_product_batches(self):
        """Create ProductBatch entities derived from processed HarvestBatch entries."""
        classes = set(self.class_info.get("classes", []))
        if "ProductBatch" not in classes or "HarvestBatch" not in classes:
            return

        rel_head, rel_tail, rel2triples = self._build_relation_index()
        processed = rel2triples.get("processedAt", [])
        if not processed:
            return

        existing = {}
        for pb, hb in rel2triples.get("batchDerivedFrom", []):
            existing[hb] = pb

        supers = self.class_info["transitive_class2superclasses"].get("ProductBatch", [])
        product_pool = list(self.class2entities.get("Product", []))
        manufacturer_pool = list(self.class2entities.get("Manufacturer", []))
        plant_pool = list(self.class2entities.get("ProcessingPlant", []))

        faker = self.faker
        used_labels = set(self.entity_labels.values()) if hasattr(self, "entity_labels") else set()

        to_add = set()
        next_id = len(self.entities) + 1
        for _, hb in processed:
            if hb in existing:
                continue
            pb_id = f"ProductBatch_{next_id}"
            while pb_id in self.entities:
                next_id += 1
                pb_id = f"ProductBatch_{next_id}"
            next_id += 1

            self.entities.append(pb_id)
            self.is_typed.add(pb_id)
            self.ent2classes_specific[pb_id] = ["ProductBatch"]
            self.ent2classes_transitive[pb_id] = list(set(["ProductBatch"] + list(supers)))
            self.class2entities.setdefault("ProductBatch", []).append(pb_id)
            for sup in supers:
                self.class2entities.setdefault(sup, [])
                if pb_id not in self.class2entities[sup]:
                    self.class2entities[sup].append(pb_id)

            if faker is not None:
                label = self._label_for_class("ProductBatch", faker)
                if label:
                    base = label
                    counter = 2
                    while label in used_labels:
                        label = f"{base} {counter}"
                        counter += 1
                    self.entity_labels[pb_id] = label
                    used_labels.add(label)

            if "batchDerivedFrom" in self.rel2dom:
                to_add.add((pb_id, "batchDerivedFrom", hb))
            if "batchOfProduct" in self.rel2dom and product_pool:
                to_add.add((pb_id, "batchOfProduct", np.random.choice(product_pool)))
            if "producesBatch" in self.rel2dom and manufacturer_pool:
                to_add.add((np.random.choice(manufacturer_pool), "producesBatch", pb_id))
            if "packagesBatch" in self.rel2dom and plant_pool:
                to_add.add((np.random.choice(plant_pool), "packagesBatch", pb_id))

        for triple in to_add:
            if self.check_consistency(triple):
                if triple not in self.kg:
                    self.kg.add(triple)
                self._register_functional_usage(*triple)

    def _enforce_org_roles(self):
        """
        Enforce role constraints:
        - operatesMill => Retailer
        - managesFarm  => Grower
        - no entity has both; drop conflicting operatesMill if entity also managesFarm.
        """
        org_subclasses = set(self.class_info.get("transitive_class2subclasses", {}).get("Organization", []))
        basic_roles = {"Grower", "Retailer", "LogisticsProvider"}
        enforce_roles = bool(org_subclasses) and org_subclasses.issubset(basic_roles)

        growers = set(self.class2entities.get("Grower", []))
        retailers = set(self.class2entities.get("Retailer", []))
        orgs = set(self.class2entities.get("Organization", []))
        logistics_providers = set(self.class2entities.get("LogisticsProvider", []))

        if logistics_providers:
            lp_triples = {
                (h, r, t)
                for h, r, t in self.kg
                if h in logistics_providers and r in ("managesFarm", "operatesMill")
            }
            if lp_triples:
                self._remove_triples(lp_triples)

        manages_heads = {h for h, r, _ in self.kg if r == "managesFarm" and h not in logistics_providers}
        operates_heads = {h for h, r, _ in self.kg if r == "operatesMill" and h not in logistics_providers}

        def set_class(ent, cls_name):
            supers = self.class_info["transitive_class2superclasses"].get(cls_name, [])
            self.ent2classes_specific[ent] = [cls_name]
            self.ent2classes_transitive[ent] = list(set([cls_name] + list(supers)))
            self.class2entities.setdefault(cls_name, [])
            if ent not in self.class2entities[cls_name]:
                self.class2entities[cls_name].append(ent)
            # ensure membership in superclasses (e.g., Organization)
            for sup in supers:
                self.class2entities.setdefault(sup, [])
                if ent not in self.class2entities[sup]:
                    self.class2entities[sup].append(ent)
            # Remove from opposite role
            if cls_name == "Grower":
                if "Retailer" in self.class2entities:
                    self.class2entities["Retailer"] = [e for e in self.class2entities["Retailer"] if e != ent]
            if cls_name == "Retailer":
                if "Grower" in self.class2entities:
                    self.class2entities["Grower"] = [e for e in self.class2entities["Grower"] if e != ent]

        to_remove = set()

        if enforce_roles:
            # Resolve conflicts: if both roles, drop operatesMill to keep Grower
            conflicts = operates_heads & manages_heads
            for h, r, t in list(self.kg):
                if h in conflicts and r == "operatesMill":
                    to_remove.add((h, r, t))

            if to_remove:
                self._remove_triples(to_remove)

            # Assign roles
            for ent in manages_heads:
                if ent in orgs:
                    set_class(ent, "Grower")
            for ent in operates_heads - conflicts:
                if ent in orgs:
                    set_class(ent, "Retailer")

        # Ensure Organization bucket is populated from transitive typing
        org_list = []
        for ent, classes in self.ent2classes_transitive.items():
            if "Organization" in classes:
                org_list.append(ent)
        self.class2entities["Organization"] = list(dict.fromkeys(org_list))

    def _prune_orphan_facilities(self):
        """Remove facilities only if they are completely disconnected from the KG."""
        facilities = set(self.class2entities.get("Facility", []))
        facilities.update(self.class2entities.get("Farm", []))
        facilities.update(self.class2entities.get("Mill", []))
        used = set()
        for h, r, t in self.kg:
            if h in facilities:
                used.add(h)
            if t in facilities:
                used.add(t)
        orphans = facilities - used
        if not orphans:
            return
        triples_to_remove = {tr for tr in self.kg if tr[0] in orphans or tr[2] in orphans}
        self._remove_triples(triples_to_remove)
        self.entities = [e for e in self.entities if e not in orphans]
        self.is_typed = {e for e in self.is_typed if e not in orphans}
        for cls in ("Facility", "Farm", "Mill"):
            if cls in self.class2entities:
                self.class2entities[cls] = [e for e in self.class2entities[cls] if e not in orphans]
        for ent in orphans:
            self.ent2classes_specific.pop(ent, None)
            self.ent2classes_transitive.pop(ent, None)
            self.literal_values_raw.pop(ent, None)

    def _build_literal(self, value, datatype, config):
        if value is None:
            return None
        dtype = (datatype or "").lower()
        if dtype == "xsd:decimal":
            decimals = config.get("decimals")
            numeric = self._coerce_numeric(value)
            if numeric is None:
                return None
            if decimals is not None:
                numeric = round(float(numeric), decimals)
            return Literal(Decimal(str(numeric)), datatype=XSD.decimal)
        if dtype == "xsd:boolean":
            return Literal(bool(value), datatype=XSD.boolean)
        if dtype == "xsd:dateTime":
            dt_value = self._parse_datetime(value)
            if not dt_value:
                return None
            return Literal(dt_value.isoformat(), datatype=XSD.dateTime)
        if dtype == "xsd:string":
            return Literal(str(value), datatype=XSD.string)
        return Literal(value)

    def _get_related_entities(self, entity, relation, inverse=False):
        if inverse:
            return [h for h, r, t in self.kg if t == entity and r == relation]
        return [t for h, r, t in self.kg if h == entity and r == relation]

    def _get_first_raw_value(self, entity, prop):
        if not entity or not prop:
            return None
        entity_values = self.literal_values_raw.get(entity)
        if not entity_values:
            return None
        values = entity_values.get(prop)
        if not values:
            return None
        if isinstance(values, list):
            return values[0]
        return values

    def _sample_truncated_normal(self, mean, stddev, min_val=None, max_val=None):
        if stddev <= 0:
            return mean
        for _ in range(12):
            sample = np.random.normal(mean, stddev)
            if (min_val is None or sample >= min_val) and (max_val is None or sample <= max_val):
                return sample
        sample = np.random.normal(mean, stddev)
        if min_val is not None:
            sample = max(sample, min_val)
        if max_val is not None:
            sample = min(sample, max_val)
        return sample

    def _sample_uniform(self, min_val, max_val):
        if min_val is None and max_val is None:
            return None
        if min_val is None:
            min_val = max_val
        if max_val is None:
            max_val = min_val
        if max_val < min_val:
            max_val = min_val
        return np.random.uniform(min_val, max_val)

    def _random_between(self, min_val, max_val, integer=False, default=None):
        if min_val is None and max_val is None:
            return default
        if min_val is None:
            min_val = max_val
        if max_val is None:
            max_val = min_val
        if integer:
            min_val = int(round(min_val))
            max_val = int(round(max_val))
            if max_val < min_val:
                max_val = min_val
            return random.randint(min_val, max_val)
        if max_val < min_val:
            max_val = min_val
        return np.random.uniform(min_val, max_val)

    def _build_timedelta(self, config, prefix="offset"):
        days = self._random_between(config.get(f"{prefix}_min_days"), config.get(f"{prefix}_max_days"), integer=True, default=0) or 0
        hours = self._random_between(config.get(f"{prefix}_min_hours"), config.get(f"{prefix}_max_hours"), default=0) or 0
        minutes = self._random_between(config.get(f"{prefix}_min_minutes"), config.get(f"{prefix}_max_minutes"), default=0) or 0
        months = self._random_between(config.get(f"{prefix}_min_months"), config.get(f"{prefix}_max_months"), integer=True, default=0) or 0
        total_days = days + months * 30
        if total_days == 0 and hours == 0 and minutes == 0:
            return None
        return dt.timedelta(days=total_days, hours=hours, minutes=minutes)

    def _sample_date_range(self, config):
        start = self._parse_datetime(config.get("start"))
        end = self._parse_datetime(config.get("end"))
        if not start or not end or end <= start:
            return None
        delta_days = (end - start).days
        if delta_days <= 0:
            return self._strip_microseconds(start)
        offset = random.randint(0, delta_days)
        return self._strip_microseconds(start + dt.timedelta(days=offset))

    def _sample_datetime_range(self, config):
        start = self._parse_datetime(config.get("start"))
        end = self._parse_datetime(config.get("end"))
        if not start or not end or end <= start:
            return None
        delta_seconds = (end - start).total_seconds()
        if delta_seconds <= 0:
            return self._strip_microseconds(start)
        random_seconds = np.random.uniform(0, delta_seconds)
        return self._strip_microseconds(start + dt.timedelta(seconds=random_seconds))

    def _parse_datetime(self, value):
        if value is None:
            return None
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, str):
            try:
                return self._strip_microseconds(dt.datetime.fromisoformat(value))
            except ValueError:
                try:
                    return self._strip_microseconds(dt.datetime.strptime(value, "%Y-%m-%d"))
                except ValueError:
                    return None
        return None

    def _coerce_numeric(self, value):
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _strip_microseconds(self, value):
        if isinstance(value, dt.datetime):
            return value.replace(microsecond=0)
        return value

    def _register_functional_usage(self, head, relation, tail):
        # Track functional / inverse-functional assertions by head/tail so we can block duplicates.
        if relation in self.functional_relations:
            self.functional_usage.setdefault(relation, {})[head] = tail
        if relation in self.inversefunctional_relations:
            self.inversefunctional_usage.setdefault(relation, {})[tail] = head

        inverse = self.rel2inverse.get(relation)
        if inverse:
            if inverse in self.functional_relations:
                self.functional_usage.setdefault(inverse, {})[tail] = head
            if inverse in self.inversefunctional_relations:
                self.inversefunctional_usage.setdefault(inverse, {})[head] = tail

        superrel = self.rel2superrel.get(relation)
        if superrel:
            if superrel in self.functional_relations:
                self.functional_usage.setdefault(superrel, {})[head] = tail
            if superrel in self.inversefunctional_relations:
                self.inversefunctional_usage.setdefault(superrel, {})[tail] = head

    def _unregister_functional_usage(self, head, relation, tail):
        if relation in self.functional_relations:
            mapping = self.functional_usage.get(relation)
            if mapping:
                mapping.pop(head, None)
        if relation in self.inversefunctional_relations:
            mapping = self.inversefunctional_usage.get(relation)
            if mapping:
                mapping.pop(tail, None)

        inverse = self.rel2inverse.get(relation)
        if inverse:
            if inverse in self.functional_relations:
                mapping = self.functional_usage.get(inverse)
                if mapping:
                    mapping.pop(tail, None)
            if inverse in self.inversefunctional_relations:
                mapping = self.inversefunctional_usage.get(inverse)
                if mapping:
                    mapping.pop(head, None)

        superrel = self.rel2superrel.get(relation)
        if superrel:
            if superrel in self.functional_relations:
                mapping = self.functional_usage.get(superrel)
                if mapping:
                    mapping.pop(head, None)
            if superrel in self.inversefunctional_relations:
                mapping = self.inversefunctional_usage.get(superrel)
                if mapping:
                    mapping.pop(tail, None)

    def _remove_triples(self, triples):
        for triple in triples:
            if triple in self.kg:
                self.kg.remove(triple)
                self._unregister_functional_usage(*triple)

    def generate_one_triple(self, r):
        """
        Generates a single triple based on the given relation.

        Args:
            self (object): The instance of the InstanceGenerator.
            r (str): The relation for which to generate the triple.

        Returns:
            tuple: A tuple containing the head entity, relation, and tail entity of the generated triple.
        """
        r2dom = self.rel2dom.get(r)
        r2range = self.rel2range.get(r)
        token_dom, token_range = False, False

        if r2dom:
            h = self._sample_unseen_entity(r2dom)
            if h is not None and self.check_class_disjointness(h, r2dom):
                token_dom = True
            else:
                is_valid = False
                attempt = 0
                while not is_valid and attempt < 10:
                    attempt += 1
                    h = self._sample_entity(r2dom)
                    if h is None:
                        break
                    is_valid = self.check_class_disjointness(h, r2dom)
                if not is_valid:
                    h = None

        else:
            if len(self.untyped_entities) > 0:
                h = (
                    self.untyped_entities_priority.pop()
                    if self.untyped_entities_priority
                    else np.random.choice(self.untyped_entities)
                )
            else:
                h = np.random.choice(self.flattened_unseen)

        if r2range:
            t = self._sample_unseen_entity(r2range)
            if t is not None and self.check_class_disjointness(t, r2range) and h is not None:
                self.class2unseen[r2range].remove(t)
                if token_dom and h in self.class2unseen[r2dom]:
                    self.class2unseen[r2dom].remove(h)
            else:
                is_valid = False
                attempt = 0
                while not is_valid and attempt < 10:
                    attempt += 1
                    t = self._sample_entity(r2range)
                    if t is None:
                        break
                    is_valid = self.check_class_disjointness(t, r2range)
                if not is_valid:
                    t = None

        else:
            if len(self.untyped_entities) > 0:
                t = (
                    self.untyped_entities_priority.pop()
                    if self.untyped_entities_priority
                    else np.random.choice(self.untyped_entities)
                )
            else:
                t = np.random.choice(self.flattened_unseen)

        if h is None or t is None:
            return (None, None, None)

        if r in self.functional_relations:
            existing = self.functional_usage.get(r, {})
            if h in existing and existing[h] != t:
                return (None, None, None)

        if r in self.inversefunctional_relations:
            existing = self.inversefunctional_usage.get(r, {})
            if t in existing and existing[t] != h:
                return (None, None, None)

        inverse = self.rel2inverse.get(r)
        if inverse:
            if inverse in self.functional_relations:
                inv_map = self.functional_usage.get(inverse, {})
                if t in inv_map and inv_map[t] != h:
                    return (None, None, None)
            if inverse in self.inversefunctional_relations:
                inv_map = self.inversefunctional_usage.get(inverse, {})
                if h in inv_map and inv_map[h] != t:
                    return (None, None, None)

        return (h, r, t)

    def check_consistency(self, triple):
        """
        Checks the consistency of a triple before adding it to the KG.

        Args:
            self (object): The instance of the InstanceGenerator.
            triple (tuple): A tuple representing a candidate triple (h, r, t).

        Returns:
            bool: True if the triple is consistent, False otherwise.
        """
        h, r, t = triple[0], triple[1], triple[2]

        if not h or not t:
            return False

        if r in self.relation_info["irreflexive_relations"] and h == t:
            return False

        if r in self.relation_info["asymmetric_relations"]:
            if h == t or (t, r, h) in self.kg:
                return False

        if r in self.functional_relations:
            existing = self.functional_usage.get(r, {})
            if h in existing and existing[h] != t:
                return False

        if r in self.inversefunctional_relations:
            existing = self.inversefunctional_usage.get(r, {})
            if t in existing and existing[t] != h:
                return False

        inverse = self.rel2inverse.get(r)
        if inverse:
            if inverse in self.functional_relations:
                inv_map = self.functional_usage.get(inverse, {})
                if t in inv_map and inv_map[t] != h:
                    return False
            if inverse in self.inversefunctional_relations:
                inv_map = self.inversefunctional_usage.get(inverse, {})
                if h in inv_map and inv_map[h] != t:
                    return False

        superrel = self.rel2superrel.get(r)
        if superrel:
            if superrel in self.functional_relations:
                sup_map = self.functional_usage.get(superrel, {})
                if h in sup_map and sup_map[h] != t:
                    return False
            if superrel in self.inversefunctional_relations:
                sup_map = self.inversefunctional_usage.get(superrel, {})
                if t in sup_map and sup_map[t] != h:
                    return False

        return True

    def check_inverseof_asymmetry(self):
        """
        Checks if the inverse-of-asymmetry condition holds for the given relations.

        This method checks if the inverse-of-asymmetry condition holds for each pair of relations (R1, R2)
        in the rel2inverse dictionary. The inverse-of-asymmetry condition states that if R1 is the inverse of R2,
        and either R1 or R2 is asymmetric, then the same (h, t) pair cannot be observed with both R1 and R2.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        rel2inverse = self.generate_rel2inverse()

        for r1, r2 in rel2inverse.items():
            if r1 in self.relation_info["asymmetric_relations"] or r2 in self.relation_info["asymmetric_relations"]:
                subset_kg = list(filter(lambda triple: triple[1] in (r1, r2), self.kg))

                if len(set(subset_kg)) < len(subset_kg):
                    counter = Counter(subset_kg)
                    duplicates = {triple for triple, count in counter.items() if count > 1}
                    self._remove_triples(duplicates)

    def check_dom_range(self):
        """
        Checks the domain and range of triples in the KG and removes inconsistent triples.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        to_remove = set()

        for triple in self.kg:
            h, r, t = triple[0], triple[1], triple[2]
            r2dom, r2range = self.rel2dom.get(r), self.rel2range.get(r)
            if r2dom and h in self.ent2classes_transitive:
                h_classes = set(self.ent2classes_transitive[h])
                is_valid = r2dom in h_classes and self.check_class_disjointness(h, r2dom)
                if not is_valid:
                    to_remove.add(triple)
            if r2range and t in self.ent2classes_transitive:
                t_classes = set(self.ent2classes_transitive[t])
                is_valid = r2range in t_classes and self.check_class_disjointness(t, r2range)
                if not is_valid:
                    to_remove.add(triple)

        self._remove_triples(to_remove)

    def generate_rel2inverse(self):
        """
        Generates a dictionary containing pairs of inverse relations.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            rel2inverse (dict): A dictionary containing the inverse of the relation.
        """
        rel2inverse = self.relation_info["rel2inverse"]
        canonical = {}
        consumed = set()

        for rel, inv in rel2inverse.items():
            if rel in consumed:
                continue
            canonical[rel] = inv
            consumed.add(rel)
            consumed.add(inv)

        return canonical

    def check_asymmetries(self):
        """
        Checks for asymmetries in the KG.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """

        for r in self.relation_info["asymmetric_relations"]:
            subset_kg = list(filter(lambda triple: triple[1] == r, self.kg))
            symmetric_dict = {}

            for triple in subset_kg:
                symmetric_triple = (triple[2], triple[1], triple[0])
                if symmetric_triple in subset_kg and symmetric_triple not in symmetric_dict.keys():
                    symmetric_dict[triple] = symmetric_triple

            to_remove = set(symmetric_dict.values())
            self._remove_triples(to_remove)

    def check_class_disjointness(self, ent, expected_class):
        """
        Checks for class disjointness (owl:disjointWith) between domain/range of a relation
        and the classes to which belong the randomly sampled entity.

        Args:
            self (object): The instance of the InstanceGenerator.
            ent (str): The entity to check.
            expected_class (str): The expected class as domain or range of a relation.

        Returns:
            bool: True if the entity classes and expected class are disjoint, False otherwise.
        """
        classes_entity_side = self.ent2classes_transitive[ent]
        classes_relation_side = self.class_info["transitive_class2superclasses"][expected_class]

        for c in classes_relation_side:
            disj = self.class2disjoints_extended.get(c, [])
            if set(disj).intersection(set(classes_entity_side)):
                return False

        return True

    def oversample_triples_inference(self):
        """
        Infers new triples to be added to the KG based on logical deductions.
        Allows reaching user-specified number of triples without increasing the number of entities.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        used_relations = set()
        id2pattern = {
            1: self.relation_info["inverseof_relations"],
            2: self.relation_info["symmetric_relations"],
            3: self.relation_info["subrelations"],
        }
        attempt = 0

        while len(self.kg) < self.num_triples:
            attempt += 1
            chosen_id = np.random.randint(1, len(id2pattern) + 1)
            pattern2rels = id2pattern[chosen_id]
            np.random.shuffle(pattern2rels)

            if pattern2rels:
                rel = pattern2rels[0]

                if rel not in used_relations:
                    attempt = 0
                    used_relations.add(rel)
                    subset_kg = set([triple for triple in self.kg if triple[1] == rel])

                    if chosen_id == 1:
                        inv_rel = self.relation_info["rel2inverse"][rel]
                        inferred_triples = inverse_inference(subset_kg, inv_rel)

                    elif chosen_id == 2:
                        inferred_triples = symmetric_inference(subset_kg)

                    elif chosen_id == 3:
                        super_rel = self.relation_info["rel2superrel"][rel]
                        inferred_triples = subproperty_inference(subset_kg, super_rel)

                    # inferred_triples = inferred_triples[: 0.5 * int(len(inferred_triples))]
                    for triple in inferred_triples:
                        if None in triple:
                            continue
                        if self.check_consistency(triple):
                            if triple not in self.kg:
                                self.kg.add(triple)
                            self._register_functional_usage(*triple)
                        if len(self.kg) >= self.num_triples:
                            return

            if attempt > 1000:
                break

    def procedure_1(self):
        """
        Checks that domains and ranges are compatible with ent2classes_transitive of instantiated triples.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """

        for rel in self.rel2dom:
            if self.rel2dom[rel] in self.class2disjoints_extended:
                subset_kg = set([triple for triple in self.kg if triple[1] == rel])
                disjoint_with_dom = self.class2disjoints_extended[self.rel2dom[rel]]
                wrong_heads = set()
                for h, _, _ in subset_kg:
                    if h in self.ent2classes_transitive:
                        intersection = set(self.ent2classes_transitive[h]).intersection(disjoint_with_dom)
                        if intersection:
                            wrong_heads.add(h)

                problematic_triples = {
                    (head, relation, tail) for head, relation, tail in subset_kg if head in wrong_heads
                }
                self._remove_triples(problematic_triples)

        for rel in self.rel2range:
            if self.rel2range[rel] in self.class2disjoints_extended:
                subset_kg = set([triple for triple in self.kg if triple[1] == rel])
                disjoint_with_range = self.class2disjoints_extended[self.rel2range[rel]]
                wrong_tails = set()
                for _, _, t in subset_kg:
                    if t in self.ent2classes_transitive:
                        intersection = set(self.ent2classes_transitive[t]).intersection(disjoint_with_range)
                        if intersection:
                            wrong_tails.add(t)

                problematic_triples = {
                    (head, relation, tail) for head, relation, tail in subset_kg if tail in wrong_tails
                }
                self._remove_triples(problematic_triples)

    def procedure_2(self):
        """
        Checks if the inverse relationship between r1 and r2 satisfies certain conditions.

        Args:
            self (object): The instance of the InstanceGenerator.

        Returns:
            None
        """
        rel2inverse = self.generate_rel2inverse()
        for r1 in rel2inverse:
            r2 = rel2inverse[r1]
            subset_kg = set([triple for triple in self.kg if triple[1] == r1])
            if r2 in self.rel2range and self.rel2range[r2] in self.class2disjoints_extended:
                range_r2 = self.rel2range[r2]
                disjoint_with = self.class2disjoints_extended[range_r2]
                wrong_heads = set()
                for h, _, _ in subset_kg:
                    if h in self.ent2classes_transitive:
                        intersection = set(self.ent2classes_transitive[h]).intersection(disjoint_with)
                        if intersection:
                            wrong_heads.add(h)

                problematic_triples = {
                    (head, relation, tail) for head, relation, tail in subset_kg if tail in wrong_heads
                }
                self._remove_triples(problematic_triples)

            if r2 in self.rel2dom and self.rel2dom[r2] in self.class2disjoints_extended:
                dom_r2 = self.rel2dom[r2]
                disjoint_with = self.class2disjoints_extended[dom_r2]
                wrong_tails = set()
                for _, _, t in subset_kg:
                    if t in self.ent2classes_transitive:
                        intersection = set(self.ent2classes_transitive[t]).intersection(disjoint_with)
                        if intersection:
                            wrong_tails.add(t)

                problematic_triples = {
                    (head, relation, tail) for head, relation, tail in subset_kg if tail in wrong_tails
                }
                self._remove_triples(problematic_triples)


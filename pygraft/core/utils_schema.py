from collections import defaultdict, deque
from pathlib import Path
import json
import yaml


def non_trivial_children(class2superclass_direct):
    """
    Returns a list of classes that have at least one non-trivial parent.
    
    Args:
        class2superclass_direct (dict): A dictionary mapping classes to their direct superclasses.
        
    Returns:
        list: A list of classes that have at least one non-trivial parent.
    """
    return [c for c in class2superclass_direct.keys() if class2superclass_direct[c] != "owl:Thing"]


def get_subclassof_count(class2layer):
    """
    Returns the number of classes that have at least one subclass.
    
    Args:
        class2layer (dict): A dictionary mapping classes to their layers.
        
    Returns:
        int: The number of classes that have at least one non-trivial parent.
    """
    return len([key for key, value in class2layer.items() if value > 1])


def get_leaves(class2superclass_direct, class2subclasses_direct):
    """
    Returns a list of classes that have no subclasses, i.e. leaves.

    Args:
        class2superclass_direct (dict): A dictionary mapping classes to their direct superclasses.
        class2subclasses_direct (dict): A dictionary mapping classes to their direct subclasses.

    Returns:
        list: A list of classes that have no subclasses.
    """
    return set(class2superclass_direct.keys()) - set(class2subclasses_direct.keys())


def get_max_depth(layer2classes):
    """
    Returns the maximum depth of the schema.

    Args:
        layer2classes (dict): A dictionary mapping layers to classes.

    Returns:
        int: The maximum depth of the schema.
    """
    return max((key for key, value in layer2classes.items() if value), default=None)


def calculate_inheritance_ratio(class2superclass_direct, class2subclasses_direct):
    """
    Calculates the inheritance ratio of the schema.

    Args:
        class2superclass_direct (dict): A dictionary mapping classes to their direct superclasses.
        class2subclasses_direct (dict): A dictionary mapping classes to their direct subclasses.

    Returns:
        float: The inheritance ratio of the schema.
    """
    n_classes = len(class2superclass_direct.keys())
    n_leaves = len(get_leaves(class2superclass_direct, class2subclasses_direct))
    n_non_trivial_children = len(non_trivial_children(class2superclass_direct))

    return n_non_trivial_children / (n_classes - n_leaves)


def calculate_average_depth(layer2classes):
    """
    Calculates the average depth of the schema.

    Args:
        layer2classes (dict): A dictionary mapping layers to classes.

    Returns:
        float: The average depth of the schema.
    """
    denominator = sum(map(len, layer2classes.values()))
    numerator = 0.0

    for key, value in layer2classes.items():
        numerator += key * len(value)

    return numerator / denominator


def calculate_class_disjointness(class2disjoint, num_classes):
    """
    Calculates the class disjointness of the schema.

    Args:
        class2disjoint (dict): A dictionary mapping classes to their disjoint classes.
        num_classes (int): The number of classes.

    Returns:
        float: The class disjointness of the schema.
    """
    return len(class2disjoint) / (2 * num_classes)


def get_all_superclasses(class_name, direct_class2superclass):
    """
    Returns a list of all superclasses of a given class.
    
    Args:
        class_name (str): The name of the class.
        direct_class2superclass (dict): A dictionary mapping classes to their direct superclasses.
        
    Returns:
        list: A list of all superclasses of the given class.
    """
    superclasses = []

    if class_name in direct_class2superclass:
        superclass = direct_class2superclass[class_name]
        superclasses.append(superclass)
        superclasses.extend(get_all_superclasses(superclass, direct_class2superclass))

    return superclasses


def get_all_subclasses(transitive_class2superclass):
    """
    Returns a dictionary mapping classes to their transitive subclasses.

    Args:
        transitive_class2superclass (dict): A dictionary mapping classes to their transitive superclasses.

    Returns:
        dict: A dictionary mapping classes to their subclasses.
    """
    class2subclasses = defaultdict(list)

    for subclass, superclasses in transitive_class2superclass.items():
        for superclass in superclasses:
            class2subclasses[superclass].append(subclass)

    return dict(class2subclasses)


def extend_class_mappings(direct_class2superclass):
    """
    Extends the class mappings to include transitive superclasses and subclasses.

    Args:
        direct_class2superclass (dict): A dictionary mapping classes to their direct superclasses.

    Returns:
        tuple: A tuple containing the extended class mappings.
    """
    transitive_class2superclass = {}
    transitive_class2subclasses = {}

    for class_name in direct_class2superclass:
        # Extend superclasses recursively
        transitive_superclasses = get_all_superclasses(class_name, direct_class2superclass)
        transitive_class2superclass[class_name] = transitive_superclasses

    transitive_class2subclasses = get_all_subclasses(transitive_class2superclass)

    return transitive_class2superclass, transitive_class2subclasses


def generate_class2layer(layer2classes):
    """
    Generates a dictionary mapping classes to their layers.

    Args:
        layer2classes (dict): A dictionary mapping layers to classes.

    Returns:
        dict: A dictionary mapping classes to their layers.
    """
    class2layer = {}

    for layer, classes in layer2classes.items():
        for c in classes:
            class2layer[c] = layer

    return class2layer


_SUPPORTED_OWL_PATTERNS = {
    "owl:Symmetric",
    "owl:Asymmetric",
    "owl:Reflexive",
    "owl:Irreflexive",
    "owl:Transitive",
    "owl:Functional",
    "owl:InverseFunctional",
}

_SUPPORTED_DATAPROPERTY_PATTERNS = {
    "owl:Functional",
}

_ALLOWED_PATTERN_CACHE = None


def _load_allowed_combinations():
    global _ALLOWED_PATTERN_CACHE
    if _ALLOWED_PATTERN_CACHE is None:
        combos_path = Path(__file__).resolve().parent / "property_checks" / "combinations.json"
        with combos_path.open() as fh:
            _ALLOWED_PATTERN_CACHE = json.load(fh)
    return _ALLOWED_PATTERN_CACHE


def _normalize_patterns(patterns, supported=None, combos=None):
    if not patterns:
        return []
    cleaned = []
    supported = supported or _SUPPORTED_OWL_PATTERNS
    for pattern in patterns:
        if pattern not in supported:
            raise ValueError(f"Unsupported OWL property: {pattern}")
        cleaned.append(pattern)
    normalized = sorted(set(cleaned))
    combos = _load_allowed_combinations() if combos is None else combos
    if combos is not None:
        key = ",".join(normalized)
        if key in combos and combos[key] != "True":
            raise ValueError(f"Incompatible OWL property combination: {key}")
        if key not in combos and len(normalized) > 1:
            raise ValueError(f"Unknown OWL property combination: {key}")
    return normalized


def _validate_classes(class_defs):
    direct_super = {}
    for cls, payload in class_defs.items():
        parents = payload.get("parents", []) or []
        if not parents:
            direct_super[cls] = "owl:Thing"
            continue
        if len(parents) > 1:
            raise ValueError(f"Multiple inheritance is not supported (class: {cls})")
        parent = parents[0]
        if parent != "owl:Thing" and parent not in class_defs:
            raise ValueError(f"Unknown parent class '{parent}' referenced by '{cls}'")
        direct_super[cls] = parent
    return direct_super


def _build_direct_subclasses(direct_super):
    direct_sub = defaultdict(list)
    for child, parent in direct_super.items():
        if parent != "owl:Thing":
            direct_sub[parent].append(child)
    return {k: sorted(v) for k, v in direct_sub.items()}


def _build_layer_map(direct_super):
    children = defaultdict(list)
    for child, parent in direct_super.items():
        if parent != "owl:Thing":
            children[parent].append(child)
    layer2classes = defaultdict(list)
    queue = deque()
    roots = sorted([cls for cls, parent in direct_super.items() if parent == "owl:Thing"])
    for root in roots:
        queue.append((root, 1))
    seen = set()
    while queue:
        cls, layer = queue.popleft()
        if cls in seen:
            continue
        seen.add(cls)
        layer2classes[layer].append(cls)
        for child in sorted(children.get(cls, [])):
            queue.append((child, layer + 1))
    return {layer: sorted(values) for layer, values in layer2classes.items()}


def _collect_disjoint_pairs(class_defs):
    direct = {}
    pairs = set()
    for cls, payload in class_defs.items():
        raw = payload.get("disjoint_with", []) or []
        for other in raw:
            if other not in class_defs:
                raise ValueError(f"Class '{cls}' declares unknown disjoint class '{other}'")
            pairs.add(tuple(sorted((cls, other))))
        if raw:
            direct.setdefault(cls, [])
            direct[cls].extend(raw)
    for a, b in list(pairs):
        direct.setdefault(a, [])
        direct.setdefault(b, [])
        if b not in direct[a]:
            direct[a].append(b)
        if a not in direct[b]:
            direct[b].append(a)
    return {k: sorted(set(v)) for k, v in direct.items()}, pairs


def _propagate_disjoint_pairs(pairs, transitive_sub):
    extended = defaultdict(set)
    for a, b in pairs:
        left = [a] + transitive_sub.get(a, [])
        right = [b] + transitive_sub.get(b, [])
        for la in left:
            for rb in right:
                if la == rb:
                    continue
                extended[la].add(rb)
                extended[rb].add(la)
    return {k: sorted(v) for k, v in extended.items()}


def _build_relation_info(relation_defs, known_classes):
    if not relation_defs:
        raise ValueError("Schema must declare at least one relation")
    relations = list(relation_defs.keys())
    rel2dom, rel2range = {}, {}
    rel2patterns, pattern2rels = {}, defaultdict(list)
    reflexive, irreflexive = [], []
    symmetric, asymmetric = [], []
    transitive, functional = [], []
    inversefunctional = []
    rel2inverse, rel2superrel = {}, {}
    inverse_pairs = set()
    subrelations = []

    for rel, payload in relation_defs.items():
        domain = payload.get("domain")
        if domain:
            if domain not in known_classes:
                raise ValueError(f"Relation '{rel}' references unknown domain class '{domain}'")
            rel2dom[rel] = domain
        rage = payload.get("range")
        if rage:
            if rage not in known_classes:
                raise ValueError(f"Relation '{rel}' references unknown range class '{rage}'")
            rel2range[rel] = rage
        patterns = _normalize_patterns(payload.get("characteristics", []))
        rel2patterns[rel] = patterns
        for pattern in patterns:
            pattern2rels[pattern].append(rel)
        if "owl:Reflexive" in patterns:
            reflexive.append(rel)
        if "owl:Irreflexive" in patterns:
            irreflexive.append(rel)
        if "owl:Symmetric" in patterns:
            symmetric.append(rel)
        if "owl:Asymmetric" in patterns:
            asymmetric.append(rel)
        if "owl:Transitive" in patterns:
            transitive.append(rel)
        if "owl:Functional" in patterns:
            functional.append(rel)
        if "owl:InverseFunctional" in patterns:
            inversefunctional.append(rel)
        inv = payload.get("inverse_of")
        if inv:
            rel2inverse[rel] = inv
        superrel = payload.get("subproperty_of")
        if superrel:
            if superrel not in relation_defs:
                raise ValueError(f"Relation '{rel}' declares unknown superproperty '{superrel}'")
            rel2superrel[rel] = superrel
            subrelations.append(rel)

    initial_inverses = dict(rel2inverse)
    for rel, inv in initial_inverses.items():
        if inv not in relation_defs:
            raise ValueError(f"Relation '{rel}' declares inverse '{inv}' which is not defined")
        rel2inverse.setdefault(inv, rel)
        inverse_pairs.add(tuple(sorted((rel, inv))))

    inverseof_relations = sorted({name for pair in inverse_pairs for name in pair})

    num_relations = len(relations)

    def ratio(count):
        return round(count / num_relations, 2) if num_relations else 0.0

    profiled_count = len(rel2dom) + len(rel2range)
    stats = {
        "num_relations": num_relations,
        "prop_reflexive": ratio(len(reflexive)),
        "prop_irreflexive": ratio(len(irreflexive)),
        "prop_functional": ratio(len(functional)),
        "prop_inversefunctional": ratio(len(inversefunctional)),
        "prop_symmetric": ratio(len(symmetric)),
        "prop_asymmetric": ratio(len(asymmetric)),
        "prop_transitive": ratio(len(transitive)),
        "prop_inverseof": ratio(len(inverseof_relations)),
        "prop_subpropertyof": round((2 * len(rel2superrel)) / num_relations, 2) if num_relations else 0.0,
        "prop_profiled_relations": round(profiled_count / (2 * num_relations), 2) if num_relations else 0.0,
        "relation_specificity": ratio(len([r for r in relations if r in rel2dom and r in rel2range])),
    }

    relation_info = {
        "statistics": stats,
        "relations": relations,
        "rel2patterns": rel2patterns,
        "pattern2rels": {k: sorted(v) for k, v in pattern2rels.items()},
        "reflexive_relations": sorted(reflexive),
        "irreflexive_relations": sorted(irreflexive),
        "symmetric_relations": sorted(symmetric),
        "asymmetric_relations": sorted(asymmetric),
        "functional_relations": sorted(functional),
        "inversefunctional_relations": sorted(inversefunctional),
        "transitive_relations": sorted(transitive),
        "inverseof_relations": inverseof_relations,
        "subrelations": sorted(subrelations),
        "rel2inverse": rel2inverse,
        "rel2dom": rel2dom,
        "rel2range": rel2range,
        "rel2superrel": rel2superrel,
    }

    return relation_info


def build_dataproperty_info(dataprop_defs, known_classes):
    dataprop_defs = dataprop_defs or {}
    dataproperties = list(dataprop_defs.keys())
    dp2dom, dp2datatype = {}, {}
    dp2patterns, pattern2dps = {}, defaultdict(list)
    dp2super = {}
    functional = []

    for dp, payload in dataprop_defs.items():
        domain = payload.get("domain")
        if domain:
            if domain not in known_classes:
                raise ValueError(f"Datatype property '{dp}' references unknown domain class '{domain}'")
            dp2dom[dp] = domain
        datatype = payload.get("datatype")
        if not datatype:
            raise ValueError(f"Datatype property '{dp}' must declare a datatype")
        dp2datatype[dp] = datatype
        patterns = _normalize_patterns(
            payload.get("characteristics", []),
            supported=_SUPPORTED_DATAPROPERTY_PATTERNS,
            combos={},
        )
        dp2patterns[dp] = patterns
        for pattern in patterns:
            pattern2dps[pattern].append(dp)
        if "owl:Functional" in patterns:
            functional.append(dp)
        superprop = payload.get("subproperty_of")
        if superprop:
            if superprop not in dataprop_defs:
                raise ValueError(f"Datatype property '{dp}' declares unknown superproperty '{superprop}'")
            dp2super[dp] = superprop

    num_dataproperties = len(dataproperties)

    stats = {
        "num_dataproperties": num_dataproperties,
        "prop_functional": round(len(functional) / num_dataproperties, 2) if num_dataproperties else 0.0,
    }

    dataproperty_info = {
        "statistics": stats,
        "dataproperties": dataproperties,
        "dp2dom": dp2dom,
        "dp2datatype": dp2datatype,
        "dp2patterns": dp2patterns,
        "pattern2dps": {k: sorted(v) for k, v in pattern2dps.items()},
        "functional_dataproperties": sorted(functional),
        "dp2super": dp2super,
    }

    return dataproperty_info


def load_schema_yaml(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("Schema YAML must define a mapping at the root")

    schema_meta = data.get("schema", {})
    schema_name = schema_meta.get("name")
    if not schema_name:
        raise ValueError("Schema metadata must include a 'name'")
    schema_format = schema_meta.get("format", "ttl")

    class_defs = data.get("classes", {})
    if not isinstance(class_defs, dict) or not class_defs:
        raise ValueError("Schema must declare at least one class")

    direct_super = _validate_classes(class_defs)
    direct_sub = _build_direct_subclasses(direct_super)
    trans_super, trans_sub = extend_class_mappings(direct_super)
    layer2classes = _build_layer_map(direct_super)
    class2layer = generate_class2layer(layer2classes)

    classes = list(class_defs.keys())
    try:
        inheritance_ratio = round(calculate_inheritance_ratio(direct_super, direct_sub), 2)
    except ZeroDivisionError:
        inheritance_ratio = 0.0
    try:
        avg_depth = round(calculate_average_depth(layer2classes), 2)
    except ZeroDivisionError:
        avg_depth = 0.0

    class2disjoints, pair_set = _collect_disjoint_pairs(class_defs)
    extended_disjoints = _propagate_disjoint_pairs(pair_set, trans_sub)
    mutual_pairs = sorted({f"{a}-{b}" for a, b in pair_set})
    avg_disjointness = (
        round(calculate_class_disjointness(class2disjoints, len(classes)), 2)
        if classes
        else 0.0
    )

    class_info = {
        "num_classes": len(classes),
        "classes": classes,
        "hierarchy_depth": get_max_depth(layer2classes) or 0,
        "avg_class_depth": avg_depth,
        "class_inheritance_ratio": inheritance_ratio,
        "direct_class2subclasses": direct_sub,
        "direct_class2superclass": direct_super,
        "transitive_class2subclasses": trans_sub,
        "transitive_class2superclasses": trans_super,
        "avg_class_disjointness": avg_disjointness,
        "class2disjoints": class2disjoints,
        "class2disjoints_symmetric": mutual_pairs,
        "class2disjoints_extended": extended_disjoints,
        "layer2classes": {int(k): v for k, v in layer2classes.items()},
        "class2layer": class2layer,
    }

    relation_defs = data.get("relations", {})
    relation_info = _build_relation_info(relation_defs, classes)

    dataproperty_defs = data.get("dataproperties", {})
    dataproperty_info = build_dataproperty_info(dataproperty_defs, classes)

    return {
        "schema": {"name": schema_name, "format": schema_format},
        "class_info": class_info,
        "relation_info": relation_info,
        "dataproperty_info": dataproperty_info,
    }

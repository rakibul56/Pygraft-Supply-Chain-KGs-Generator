import pickle
import json
import pathlib
import tempfile
from rdflib import Graph as RDFGraph
from owlready2 import *
import owlready2
from art import *
import random
import os
import yaml
from datetime import datetime
import pkg_resources
import shutil

font_styles = ["dancingfont", "rounded", "varsity", "wetletter", "chunky"]


def print_ascii_header():
    """
    
    """
    header = text2art("PyGraft", font=random.choice(font_styles))
    print("\n")
    print(header)
    print("\n")


def initialize_folder(folder_name):
    """
    Initializes a folder for output files.

    Args:
        self (object): The instance of the SchemaBuilder.
        folder_name (str): The name of the folder to be created. If None, a folder with the current date and time will be created.

    Returns:
        None
    """
    output_folder = "output/"
    if folder_name is None or folder_name == "None":
        folder_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    output_folder += folder_name

    directory = f"{output_folder}/"
    if not os.path.exists(directory):
        os.makedirs(directory)

    return folder_name


def load_config(path):
    """
    Loads a configuration from a JSON or YAML file.
    
    Args:
        path (str): The path to the configuration file.

    Raises:
        ValueError: If the configuration file format is not supported.
        
    Returns:
        dict: The configuration dictionary.
    """
    path = pathlib.Path(path)

    if path.suffix == ".json":
        with path.open() as file:
            return json.load(file)

    if path.suffix in {".yaml", ".yml"}:
        with path.open() as file:
            return yaml.safe_load(file)

    raise ValueError(f"Unknown configuration file format: {path.suffix}. Valid formats: .json, .yaml, .yml")


def get_most_recent_subfolder(folder_path):
    """
    Gets the most recent subfolder in the given folder path.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        str or None: The name of the most recent subfolder or None if there are no subfolders.
    """
    subfolders = [f.path for f in os.scandir(folder_path) if f.is_dir()]
    if not subfolders:
        return None
    most_recent_subfolder = max(subfolders, key=os.path.getctime)

    return os.path.basename(most_recent_subfolder)


def check_schema_arguments(config):
    """
    Checks the validity of the schema arguments.

    Args:
        config (dict): The configuration dictionary.

    Raises:
        AssertionError: If the proportions of owl:Asymmetric and owl:Symmetric relations sum to more than 1,
                        or if the proportions of owl:Irreflexive and owl:Reflexive relations sum to more than 1.
        AssertionError: If the current PyGraft version does not handle rdfs:subPropertyOf, owl:FunctionalProperty,
                        and owl:InverseFunctionalProperty at the same time.

    Returns:
        None
    """
    assert (
        config["prop_symmetric_relations"] + config["prop_asymmetric_relations"] <= 1.0
    ), "Proportions of owl:Asymmetric and owl:Symmetric relations cannot sum to more than 1."
    assert (
        config["prop_reflexive_relations"] + config["prop_irreflexive_relations"] <= 1.0
    ), "Proportions of owl:Irreflexive and owl:Reflexive relations cannot sum to more than 1."
    assert (
        config["avg_class_depth"] < config["max_hierarchy_depth"]
    ), "The average class depth value cannot be set higher than the class hierarchy depth."

    assert (
        config["prop_subproperties"] == 0.0
        and (config["prop_functional_relations"] >= 0.0 or config["prop_inverse_functional_relations"] >= 0.0)
    ) or (
        config["prop_subproperties"] >= 0.0
        and (config["prop_functional_relations"] == 0.0 or config["prop_inverse_functional_relations"] == 0.0)
    ), """
    The current PyGraft version does not handle rdfs:subPropertyOf, owl:FunctionalProperty, and owl:InverseFunctionalProperty **at the same time**.
    Retry choosing either:
    (1) -psub 0.0     -pfr value1  -pifr value2
    (2) -psub value3  -pfr 0.0     -pifr 0.0
    """


def check_kg_arguments(config):
    """
    Checks the validity of the knowledge graph arguments.

    Args:
        config (dict): The configuration dictionary.
    
    Returns:
        None
    """

    if config["multityping"] == False:
        config["avg_multityping"] = 1.0

    # Define default value to run pygraft.utils.reasoner
    if "kg_check_reasoner" in config.keys():
        print(f"\nkg_check_reasoner {config['kg_check_reasoner']}.\n")
    else:
        print(f"\nkg_check_reasoner not defined, setting to True.\n")
        config["kg_check_reasoner"] = True



    
def set_reasoner_java_memory_mb(java_memory_mb):
    if java_memory_mb is None:
        return
    if isinstance(java_memory_mb, bool):
        return
    value = None
    if isinstance(java_memory_mb, (int, float)):
        value = int(java_memory_mb)
    elif isinstance(java_memory_mb, str):
        raw = java_memory_mb.strip().lower()
        if raw:
            multiplier = 1
            if raw.endswith("g"):
                multiplier = 1024
                raw = raw[:-1]
            elif raw.endswith("m"):
                raw = raw[:-1]
            try:
                value = int(float(raw) * multiplier)
            except ValueError:
                value = None
    if not value or value <= 0:
        return
    try:
        from owlready2 import reasoning as owl_reasoning
        owl_reasoning.JAVA_MEMORY = value
    except Exception:
        return
    if hasattr(owlready2, "JAVA_MEMORY"):
        owlready2.JAVA_MEMORY = value


def reasoner(resource_file=None, infer_property_values=False, debug=False, keep_tmp_file=False, resource="schema", java_memory_mb=None):
    """Runs the HermiT reasoner on the given OWL file."""
    if resource_file is None:
        raise ValueError("resource_file must be provided to run the reasoner")
    if java_memory_mb is not None:
        set_reasoner_java_memory_mb(java_memory_mb)

    serialized_path = pathlib.Path(resource_file)
    cleanup_path = None

    if serialized_path.suffix.lower() in {".ttl", ".nt", ".n3", ".jsonld"}:
        rdf_graph = RDFGraph()
        rdf_graph.parse(str(serialized_path))
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".owl")
        rdf_graph.serialize(tmp.name, format="xml")
        tmp.close()
        cleanup_path = pathlib.Path(tmp.name)
        serialized_path = cleanup_path

    graph = get_ontology(str(serialized_path)).load()

    try:
        sync_reasoner_hermit(
            graph, infer_property_values=infer_property_values, debug=debug, keep_tmp_file=keep_tmp_file
        )
        print(f"\nConsistent {resource}.\n")
    except OwlReadyInconsistentOntologyError as exc:
        msg = f"Inconsistent {resource} detected by HermiT."
        raise RuntimeError(msg) from exc
    finally:
        graph.destroy()
        if cleanup_path and not keep_tmp_file:
            os.remove(str(cleanup_path))

def save_dict_to_text(data_dict, file_path):
    """
    Saves a dictionary to a text file.
    
    Args:
        data_dict (dict): The dictionary to be saved.
        file_path (str): The path to the file.
        
    Returns:
        None
    """
    with open(file_path, "w") as file:
        for k, v in data_dict.items():
            if isinstance(v, list):
                for v2 in v:
                    file.write(str(k) + "\t" + str(v2) + "\n")
            else:
                file.write(str(k) + "\t" + str(v) + "\n")


def save_dict_to_pickle(data_dict, file_path):
    """
    Saves a dictionary to a pickle file.

    Args:
        data_dict (dict): The dictionary to be saved.
        file_path (str): The path to the file.
    
    Returns:
        None
    """
    with open(file_path, "wb") as file:
        pickle.dump(data_dict, file)


def save_set_uris_to_text(set_uris, file_path):
    """
    Saves a set of triples to a text file.
    
    Args:
        set_uris (set): The set of triples to be saved.
        file_path (str): The path to the file.
        
    Returns:
        None
    """
    with open(file_path, "w") as file:
        for t in set_uris:
            file.write(f"""<{t[0]}> <{t[1]}> <{t[2]}> .\n""")


def save_set_ids_to_text(set_ids, file_path):
    """
    Saves a set of triples to a text file.

    Args:
        set_ids (set): The set of triples to be saved.
        file_path (str): The path to the file.

    Returns:
        None
    """
    with open(file_path, "w") as file:
        for t in set_ids:
            file.write(f"""{t[0]}\t{t[1]}\t{t[2]}\n""")


def load_json(file_path):
    """
    Loads a JSON file.

    Args:
        file_path (str): The path to the file.

    Returns:
        dict: The loaded JSON file.
    """
    path = pathlib.Path(file_path)
    with path.open() as file:
        return json.load(file)


def load_json_template():
    """
    Loads a JSON file.
    
    Args:
        file_path (str): The path to the file.
        
    Returns:
        dict: The loaded JSON file.
    """
    json_file_path = pkg_resources.resource_filename("pygraft", "examples/template.json")
    destination_directory = os.getcwd()
    # Use the 'cp' command to copy the file
    # subprocess.run(["cp", json_file_path, destination_directory])
    shutil.copy(json_file_path, destination_directory)


def load_yaml_template():
    """
    Loads a YAML file.

    Args:
        file_path (str): The path to the file.

    Returns:
        dict: The loaded YAML file.
    """
    yaml_file_path = pkg_resources.resource_filename("pygraft", "examples/template.yml")
    destination_directory = os.getcwd()
    # Use the 'cp' command to copy the file
    # subprocess.run(["cp", yaml_file_path, destination_directory])
    shutil.copy(yaml_file_path, destination_directory)

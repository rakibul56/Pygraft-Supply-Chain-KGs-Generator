from setuptools import setup, find_packages

setup(
    name="pygraft",
    version="0.1.0",
    description="A Python library for knowledge graph generation",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "click",
        "faker",
        "rdflib",
        "pyyaml",
        "owlrl",
        "jpype1",
    ],
    entry_points={
        "console_scripts": [
            "pygraft-sc=pygraft.cli.pygraft_sc:main",
        ],
    },
    include_package_data=True,
    package_data={
        "pygraft": [
            "domains/*/config/*.yaml",
            "domains/*/constraints/*.ttl",
            "domains/*/ontology/*.yaml",
            "core/examples/*.json",
            "core/examples/*.yml",
            "core/property_checks/*.json",
            "core/property_checks/*.txt",
        ],
    },
)
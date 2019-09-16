from setuptools import setup

setup(
    name="matchengine",
    version="2.0.0",
    packages=[
        "matchengine",
        "matchengine.internals",
        "matchengine.internals.database_connectivity",
        "matchengine.internals.plugin_helpers",
        "matchengine.internals.typing",
        "matchengine.internals.utilities",
        "matchengine.plugins"
    ],
    entrypoints={
        "console-scripts": [
            "matchengine =  matchengine.main"
        ]
    },
    install_requires=[
        "bson>=0.5.8",
        "python-dateutil==2.8.0",
        "PyYAML==5.1",
        "Pandas>=0.25.0",
        "pymongo==3.8.0",
        "networkx==2.3",
        "motor==2.0.0"
    ],
    include_package_data=True
)

from setuptools import setup

setup(
    name="MatchEngine V2",
    version="0.0.1",
    packages=["matchengine"],
    entrypoints = {
        "console-scripts": [
            "matchengine =  matchengine.main"
        ]
    }
)

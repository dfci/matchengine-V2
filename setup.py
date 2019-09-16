from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="matchengine-V2",
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
    author='Eric Marriott, Ethan Siegel',
    author_email='marriott@ds.dfci.harvard.edu, esiegel@ds.dfci.harvard.edu',
    description='Open source engine for matching cancer patients to precision medicine clinical trials (V2).',
    long_description=long_description,
    long_description_content_type='text/markdown',
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
    include_package_data=True,
    python_requires='>=3.7',
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Operating System :: POSIX :: BSD",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "Topic :: Utilities",
        "Typing :: Typed"
    ]
)

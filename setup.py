from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="MatchEngine V2",
    version="2.1.1",
    packages=find_packages(),
    package_data={'matchengine': ['ref/*', 'config/*', 'plugins/*']},
    author='Eric Marriott, Ethan Siegel',
    author_email='esiegel@ds.dfci.harvard.edu',
    description='Open source engine for matching cancer patients to precision medicine clinical trials (V2).',
    long_description=long_description,
    entry_points={
        "console_scripts": [
            "matchengine = matchengine.main:run_cli"
        ]
    },
    install_requires=[
        "python-dateutil>=2.8.0",
        "PyYAML>=5.1",
        "pymongo>=3.8.0,<4",
        "networkx>=2.3",
        "motor==2.0.0"
    ],
    include_package_data=True,
    python_requires='>=3.7',
    download_url='https://github.com/dfci/matchengine-V2/archive/2.0.0.tar.gz',
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

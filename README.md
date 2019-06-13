# MatchEngine
Welcome to the documentation for the MatchEngine! The MatchEngine is designed to match cancer patients to genomically driven, precision medicine clinical trials. 

## Getting Started 
To get started, first setup a mongo database, then load example trial and patient data. 

##### Step 1: Setup:
First get a MongoDB instance up and running.

`docker run -d -p 27017:27017 --name=mongo mongo `

For more details on how to get a MongoDB started on Linux, Windows, or MacOS please see [their installation page](https://docs.mongodb.com/manual/administration/install-community/)

Then, make sure you have a python virtual environment setup running Python >=3.7. 
```
virtualenv ~/mm_engine
. ~/mm_engine/bin/activate
```
If you run into issues, see [this tutorial](https://docs.python-guide.org/dev/virtualenvs/) for more details on setting up a virtual environment 

##### Step 2: Load trial data:
Included in the repo are 3 example clinical trials structured using [Clinical Trial Markup Language (CTML)](#CTML). Load these trials into your mongo database.

```python matchengine.py load --mongo-uri=mongodb://localhost:27017/matchminer -trials ./example_data/trials/  ```

##### Step 3: Load example patient data:
Included in the repo are 5 example patients with clinical and genomic attributes chosen to match the eligibility criteria as specified in the example trials. Load these patients data into your mongo database

```python matchengine.py load --mongo-uri=mongodb://localhost:27017/matchminer -c ./example_data/patients/clinical/ -g ./example_data/patients/genomic/```

All criteria used in matching may be configured externally and require no alteration to the source code. For convenience, the example data has been setup to work out of the box, butfFor more details on configuring the matchengine to work on custom matching criteria, please see [Data Transformation and Configuration](#Data Transformation and Configuration)

## Overview
The MatchEngine uses MongoDB as a database, and ingests structured clinical trial data using using Clinical Trial Markup Language (CTML), and patient data. 

### CTML

### Patient Data

### Trial Match Output

## Configuration

### Data Transformation and Configuration

## Built with
* [MongoDB](https://docs.mongodb.com/) - NoSQL document database for data storage.
* [nose](http://nose.readthedocs.io/en/latest/) - Python library for unit testing.
* Python3


## Authors
* **Eric Marriott**
* **Ethan Siegel**
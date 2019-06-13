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
If you run into issues, see [this tutorial](https://docs.python-guide.org/dev/virtualenvs/) for more details on setting up a python virtual environment 

##### Step 2: Load trial data:
Included in the repo are 3 example clinical trials structured using [Clinical Trial Markup Language (CTML)](#CTML). Load these trials into your mongo database.

```python matchengine.py load --mongo-uri mongodb://localhost:27017/matchminer -trials ./example_data/trials/  ```

##### Step 3: Load example patient data:
Included in the repo are 5 example patients with clinical and genomic attributes chosen to match the eligibility criteria as specified in the example trials. Load these patients data into your mongo database

```python matchengine.py load --mongo-uri mongodb://localhost:27017/matchminer -c ./example_data/patients/clinical/ -g ./example_data/patients/genomic/```

All criteria used in matching may be configured externally and require no alteration to the source code. For convenience, the example data has been setup to work out of the box, but for more details on configuring the matchengine to work on custom matching criteria, please see [Data Transformation and Configuration](#Data Transformation and Configuration)

##### Step 4: Run the engine
Now that the data has been loaded, run the matchengine to generate trial matches
```python matchengine.py match --mongo-uri mongodb://localhost:27017/matchminer```

This command should generate documents in a new collection in your database `trial_match_raw`, as well as 3 records in a collection called `matchengine_run_log` with details on the the number of trials ran, and the sample ids which were changed as a result of that run. 

##### Step 5: Create a db view
The documents which exist in the `trial_match_raw` collection are unsorted and must be aggregated in order to be ingestable by the MatchMinerUI. To do so, first access your mongo shell:  
```
docker exec -it mongo mongo
show dbs 
use matchminer
```

Then, run this command to create a view called `trial_match`. This view functions like a table and will represent a sorted version of the data generated and deposited into `trial_match_raw`

```
db.trial_match.aggregate({
}
)
```  

These are the output documents which need to be aggregated  
 

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
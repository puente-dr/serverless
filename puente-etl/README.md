# Puente ETL


## Project Structure

- `/data-aggregation` contains logic for connection to make REST API call to 
  [parse-server](https://docs.parseplatform.org/rest/guide/#getting-started) is being used to reduce boilerplate.


## Build 
**Install virtual environment library**
```
pip install virtualenv #if you don't have virtualenv installed 
```

**Create virtualenv**
```
virtualenv <Name_of_Virtual_Environment>
```
***NOTE***
Here's how to install a specific version
```
virtualenv -p python3.9 <Name_of_Virtual_Environment>
```

**Activate virtualenv**
```
source <Name_of_Virtual_Environment>/bin/activate
```

**Install project requirements usings the requirements.text**
```
pip install -r requirements.txt
```

## Run locally
Run python script in etl directory to start
_Make sure you have your .env is set_
```
cd puente-etl/lambda/etl
python3 extract_back4app_data.py
python3 jobs.py
```
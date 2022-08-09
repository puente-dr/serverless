# Puente ETL

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
python3 lambdas/etl/index.py
```

# Resource
- [Creating a layer](https://aws.amazon.com/premiumsupport/knowledge-center/lambda-import-module-error-python/#:~:text=This%20is%20because%20Lambda%20isn,Python%20inside%20the%20%2Fpython%20folder.)
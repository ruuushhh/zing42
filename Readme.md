# Internship Assignment with FastAPI (Zing42)

Following task is done in FastAPI to ease the task of testing and understanding it in a better way
I have used MongoDB as a database.


```bash
#install virtual environment
pipenv shell

# Install the requirements:
pipenv install 

# Start the service:
uvicorn app:app --reload
```


# Query

This queries are written for relational database like mySql, etc.


```bash

select TOP 25 * from BhavCopy Order by ((close-open)/open);

```

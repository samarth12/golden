# Golden Challenge

Documentation for the Golden take home challenge

## Challenge 1 - Importing

The `upload_to_db.py` script contains two functions - 

1. `preprocess_csv()` to preprocess the data and save it in a format that is digestable by the DB. 
2. `upload_data_to_database()` to upload the data directly to a MYSQL database hosted on AWS RDS. 

### Key Points

#### Database
1. I selected a MySQL database because the task was related to "querying" the data in different ways. I just felt that MySQL would be a better choice for a frequent query related application. There is a predefined schema and the queries involve multi row transactions. 
2. I am leveraging AWS to host a MySQL database using AWS' RDS service. It is easy to deploy, scale, maintain and monitor. It is available all the time and supports auto replication and many other features. 
3. In order to insert the data as fast as possible, instead of choosing a rowise insertion - I used the `LOAD DATA INLINE` functionality which decreases the insertion time exponentially. 
4. In an effort to further improve the query time I set the `Neighborhoods` column as an index as it is used as a filter in the second and third query. 
5. For insertion related metrics, I print the following - total insertion time, number of rows inserted, rows inserted per second and the total size of the database.
6. `console_output.txt` contains the console output from when you run the `upload_to_db.py` script. `console_output.jpg` is a screenshot of the same.  
7. The AWS RDS dashboard gives an in depth insight into some more sophisticated metrics like read/write operations, throughput, CPU load, etc. 

#### CSV Data

1. I used pandas to manipulate the data - to clean and filter it. 
2. I only filter the relevant columns.
3. I only filtered row where CITY==San Francisco. 
4. The data is stored in the `data` folder. 

## Challenge 2 - Rest API

I used AWS API Gateway to build the REST API. I used Lambda behind the gateway to process incoming request and accordingly query the database. I chose AWS to enable myself for quick testing and deployment. 

The response is pretty printed. The `swagger` folder contains swagger format for the API in json and YAML format. 

### Query 1

Link and working example - 

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query1
```

The link returns the output for the query -  `Which San Francisco neighborhoods are there _Business Location_ records for?`. The query used can be found in `lambda_handler.py` in the `run_query1()` function. 

### Query 2

Link - 

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query2/{neighborhood_name}?order={shortest/longest}
```

You can enter the `neigborhood_name` and the `order` based on the query.

Working example for longest existing locations for neighborhood `Marina` - 

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query2/Marina?order=longest
```

Working example for shortest existing locations for neighborhood `Marina` - 

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query2/Marina?order=shortest
```

The link returns the output for the query -  `Given a neighborhood, what are the _DBA Names_ and full addresses of the top 100 longest(shortest)-existing locations that haven't ended operations?`. The query used can be found in `lambda_handler.py` in the `run_query2()` function.


### Query 3

Link -

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query3/{neighborhood_name}
```

You can enter the `neigborhood_name` based on the query. 


Working example for neighborhood `Potrero Hill` - 

```
https://i0btc4oss2.execute-api.us-west-1.amazonaws.com/prod/query3/Potrero Hill
```

The link returns the output for the query -  ` What is the geographic center (latitude and longitude) of all the businesses of a given neighborhood?`. The query used can be found in `lambda_handler.py` in the `run_query3()` function.

## Run Instructions

The API endpoints are accessible through the links posted above. The code lives in AWS Lambda, but I have attached the code in this repository for reference.  I would be more than happy to walk you through the specifics if needed. 

To run `upload_to_db.py`, you require python 3. I used python `3.7.5` but it should work with any version. There are different ways of installing it based on your OS. I developed this on MacOS Big Sur. I usually use [Homebrew](https://docs.brew.sh/Installation) and `pyenv` for the installation. I am skipping python installation instructions as it would depend on the OS and its versions, especially for Mac (you might require XCode, Big Sur has trouble installing 3.7.5).

To install other dependencies after installing python, run - 

```
pip install -r requirements.txt
```

To run the upload script - 

```
python upload_to_db.py
```


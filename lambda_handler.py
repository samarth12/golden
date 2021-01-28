import json
import sys

import pymysql

rds_host = "golden.cpwfu1xlyexe.us-west-1.rds.amazonaws.com"


def run_query1(cursor, event):
    response = {}
    cursor.execute("SELECT DISTINCT Neighborhoods "
                   "FROM sf_gov_data "
                   "USE INDEX (neighborhood_index) "
                   "WHERE Neighborhoods IS NOT NULL")
    neighborhoods = cursor.fetchall()
    neighborhoods_list = [list(neighborhood.items())[0][-1] for neighborhood in neighborhoods]
    response['neighborhoods'] = neighborhoods_list
    response['path'] = event['path']
    response['query'] = "Which San Francisco neighborhoods are there _Business Location_ records for?"
    return response


def run_query2(cursor, event, neighborhood):
    response = {}

    if event['queryStringParameters']['order'] != 'shortest' and event['queryStringParameters']['order'] != 'longest':
        response[
            'message'] = f"{event['queryStringParameters']['order']} is not a valid option. Please use 'shortest' or " \
                         f"'longest' as an option for 'order'"
        response['statusCode'] = 422
        return response

    order = 'asc' if event['queryStringParameters']['order'] == 'shortest' else 'desc'

    cursor.execute(
        "SELECT DBA_Name, Street_Address, City, State, Source_Zipcode, DATEDIFF(NOW(), Business_Start_Date) "
        "AS days_active "
        "FROM sf_gov_data "
        "USE INDEX (neighborhood_index) "
        f"WHERE Business_End_Date IS NULL AND Neighborhoods = '{neighborhood}' "
        f"ORDER BY days_active {order} limit 100")
    names = cursor.fetchall()
    response['neighborhood'] = neighborhood
    response['DBA_Names'] = names
    response['path'] = event['path']
    response['queryParameter'] = event['queryStringParameters']['order']
    response[
        'query'] = "Given a neighborhood, what are the _DBA Names_ and full addresses of the top 100 longest " \
                   "(shortest)-existing locations that haven't ended operations?"

    return response


def run_query3(cursor, event, neighborhood):
    response = {}
    cursor.execute(f"SELECT SUM(x_coordinate_location)/COUNT(*) AS geographic_center_x, "
                   "SUM(y_coordinate_location)/COUNT(y_coordinate_location) AS geographic_center_y "
                   "FROM sf_gov_data "
                   "USE INDEX (neighborhood_index) "
                   f"WHERE Neighborhoods = '{neighborhood}'")
    coordinates = cursor.fetchall()
    response['neighborhood'] = neighborhood
    response['geographic_center_x'] = coordinates[0]['geographic_center_x']
    response['geographic_center_y'] = coordinates[0]['geographic_center_y']
    response['path'] = event['path']
    response[
        'query'] = "What is the geographic center (latitude and longitude) of all the businesses of a given " \
                   "neighborhood?"

    return response


def lambda_handler(event, context):
    path_list = event['path'].split("/")
    print(event)
    try:
        connection = pymysql.connect(host=rds_host,
                                     user='admin',
                                     passwd='password',
                                     db='golden_DB',
                                     connect_timeout=5,
                                     cursorclass=pymysql.cursors.DictCursor)

    except Exception as e:
        print(e)
        sys.exit()

    cursor = connection.cursor()

    response = {}
    if path_list[1] == 'query1':
        response = run_query1(cursor, event)

    elif path_list[1] == "query2":
        neighborhood = "/".join(path_list[2:])
        neighborhood = neighborhood.replace("%20", ' ')
        response = run_query2(cursor, event, neighborhood)

    elif path_list[1] == "query3":
        neighborhood = "/".join(path_list[2:])
        neighborhood = neighborhood.replace("%20", ' ')
        response = run_query3(cursor, event, neighborhood)

    response_object = {}
    response_object['statusCode'] = 200
    response_object['headers'] = {}
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['body'] = json.dumps(response, indent=4)

    return response_object

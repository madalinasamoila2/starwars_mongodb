## PROJECT
# The data in this database has been pulled from SWAPI - A New Hope. As well as 'people', the API has data on starships. In Python, pull data on all available starships from the API. The "pilots" key contains URLs pointing to the characters who pilot the starship. Use these to replace 'pilots' with a list of ObjectIDs from our characters collection, then insert the starships into their own collection. 
# Deliverables:
# Your python file should: query the API, retrieve the starships (only keep useful fields). Transform the pilot list to contain a list of ObjectIds for the relative characters. Load the final starship documents into a new collection in MongoDB.
# Extensions for project:
# Improve your code by adding error handling, commenting, unit testing, etc.

import requests
import json
import pymongo

response = requests.get("https://www.swapi.tech/api/starships")
data = response.json()
print(data['results'])

#### 1. Pull data on all available starships from the API
# FUNCTION TO RETRIEVE DATA FROM API
def fetch_api_data(url):
    """Fetches api data from the provided URL and returns the results"""
    # we want to add all dictionairies to a list starship_dict
    starship_list = []
    try:    
        while url:
            # get url data using requests
            response = requests.get(url)
            data = response.json()

            # get the list of starships from the current page
            starship_list.extend(data["results"])

            # get the list of starships from the next page
            url = data["next"]

        # make the data look pretty
        # pretty_starship_list = json.dumps(starship_list, indent=4)
        return starship_list
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API {e}")
        return None

starship = fetch_api_data("https://www.swapi.tech/api/starships")
pretty_starship = json.dumps(starship, indent=4)
print(pretty_starship)

#### 2. Fetch data for each starship in a loop.
# QUERYING STARSHIP DETAILS
detailed_starships = []

for ship in starship:
    detail_url = ship['url']
    try:
        response = requests.get(detail_url)
        response.raise_for_status
        detail_data = response.json()
        # append the starship details info inside detailed_starships
        detailed_starships.append(detail_data['result']['properties'])
        pretty_detailed_starships = json.dumps(detailed_starships, indent=4)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API {e}")
print(pretty_detailed_starships)
print(f"\nTotal detailed starships fetched: {len(detailed_starships)}")

#### 3. Retrieve pilot information for each starship
pilot_list = []

for ship in detailed_starships:
    pilot_urls = ship['pilots']
    if pilot_urls:
        for url in pilot_urls:
            try:
                response = requests.get(url)
                pilot_url = response.json()
                # append the pilot list to include information about pilots
                pilot_list.append(pilot_url)
                pretty_pilot = json.dumps(pilot_list, indent=4)
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch details for {detail_url}: {e}")
print(pretty_pilot)
            

#### 3. Replace "pilots" key URL with a list of object IDs from the characters collection (people who pilot the starship) and insert into their own collection.
pilot_names = []

for pilot in pilot_list:
    try:
        pilots = pilot['result']['properties']['name']
        pilot_names.append(pilots)
    except KeyError as e:
        print(f"Missing key: {e} in pilot data")
print(pilot_names)

#### 4. Print out all Object IDs based on character
# GET DATA FROM MONGON DB FULL_CHARACTERS
client = pymongo.MongoClient() # CLASS from pymongo, hosts the database connection for usin mongodb://localhost:27017 for us
db = client['starwars']

object_ids = []

for character in pilot_names:
    result = db.full_characters.find({"name":character},{'_id':1,'name':1})
    for doc in result:
        object_ids.append(doc)
        print(doc)

#### 5. Transform the pilot list to contain a list of ObjectIds for the relative characters. 
# what should the pilot list contain apart from the name/ object ids?

#### 6. Load the final starship document into a new collection in MongoDB.
# only use necessary fields i.e uid, name,
        # "created": "2025-05-21T22:46:50.172Z",
        # "edited": "2025-05-21T22:46:50.172Z",
        # "consumables": "1 year",
        # "name": "CR90 corvette",
        # "cargo_capacity": "3000000",
        # "passengers": "600",
        # "max_atmosphering_speed": "950",
        # "crew": "30-165",
        # "length": "150",
        # "model": "CR90 corvette",
        # "cost_in_credits": "3500000",
        # "manufacturer": "Corellian Engineering Corporation",
        # "pilots": [],
        # "MGLT": "60",
        # "starship_class": "corvette",
        # "hyperdrive_rating": " 

# from pilots just their name and object id?
import requests
import json
from pymongo import MongoClient

# -----------------------------------------
# 1. Fetch all starships from the SWAPI API
# -----------------------------------------

def fetch_api_data(url):
    """Fetch all starships from the SWAPI API"""
    starship_list = []
    try:
        while url:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            starship_list.extend(data["results"])
            url = data["next"]
        return starship_list
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except ValueError as ve:
        print(f"JSON parsing error: {ve}")
    except KeyError as ke:
        print(f"Missing expected key: {ke}")
    return []

# -----------------------------------------
# 2. Get detailed data for each starship
# -----------------------------------------

def get_detailed_starships(starships):
    """Retrieve full details for each starship."""
    detailed_starships = []
    for ship in starships:
        try:
            response = requests.get(ship['url'])
            response.raise_for_status()
            starship_data = response.json()
            detailed_starships.append(starship_data['result']['properties'])
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch details for {ship['url']}: {e}")
    return detailed_starships

# -----------------------------------------
# 3. Filter required fields from each starship
# -----------------------------------------

def filter_starship_fields(detailed_starships):
    """Filter only necessary fields from the detailed starship data."""
    keys_to_keep = [
        "name", "manufacturer", "cargo_capacity", "length",
        "max_atmosphering_speed", "cost_in_credits", "crew",
        "passengers", "pilots"
    ]
    filtered = []
    for starship in detailed_starships:
        try:
            filtered.append({key: starship[key] for key in keys_to_keep})
        except KeyError as e:
            print(f"Missing key during filtering: {e}")
    return filtered

# -----------------------------------------
# 4. Replace pilot URLs with names
# -----------------------------------------

def resolve_pilot_names(starships):
    """Replace pilot URLs with a list of pilot names."""
    for ship in starships:
        pilot_urls = ship.get('pilots', [])
        pilot_names = []
        for url in pilot_urls:
            try:
                response = requests.get(url)
                response.raise_for_status()
                pilot_data = response.json()
                name = pilot_data['result']['properties']['name']
                pilot_names.append({"name": name})
            except requests.exceptions.RequestException as e:
                print(f"Error fetching pilot data from {url}: {e}")
        ship['pilots'] = pilot_names
    return starships

# -----------------------------------------
# 5. Replace pilot names with MongoDB ObjectIds
# -----------------------------------------

def map_pilots_to_object_ids(starships, db):
    """Map pilot names to their ObjectIds in MongoDB."""
    for ship in starships:
        pilot_entries = []
        for pilot in ship.get('pilots', []):
            name = pilot.get('name')
            result = db.full_characters.find_one({"name": name}, {"_id": 1})
            pilot_entries.append({
                "name": name,
                "_id": str(result["_id"]) if result else None
            })
        ship['pilots'] = pilot_entries
    return starships

# -----------------------------------------
# 6. Insert final starship documents into MongoDB
# -----------------------------------------

def insert_starships_into_mongodb(starships, db):
    """Insert the starships into the MongoDB 'starships' collection."""
    try:
        db.starships.delete_many({})
        db.starships.insert_many(starships)
        print(f"Inserted {len(starships)} starships into MongoDB.")
    except Exception as e:
        print(f"Failed to insert starships: {e}")

# -----------------------------------------
# MAIN EXECUTION
# -----------------------------------------

def main():
    # MongoDB setup
    client = MongoClient()
    db = client['starwars']

    # Step 1: Fetch base starship data
    base_starships = fetch_api_data("https://www.swapi.tech/api/starships")
    if not base_starships:
        print("No starships fetched.")
        return

    # Step 2: Get full starship details
    detailed_starships = get_detailed_starships(base_starships)

    # Step 3: Filter for necessary fields
    filtered_starships = filter_starship_fields(detailed_starships)

    # Step 4: Replace pilot URLs with names
    resolved_starships = resolve_pilot_names(filtered_starships)

    # Step 5: Replace pilot names with ObjectIds from MongoDB
    final_starships = map_pilots_to_object_ids(resolved_starships, db)

    # Step 6: Insert final documents into MongoDB
    insert_starships_into_mongodb(final_starships, db)

if __name__ == "__main__":
    main()

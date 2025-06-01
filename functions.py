import csv, requests
import time


# Run this function to fetch a list of resource IDs for BCN data packages with multiple datasets.
# If you visit the page of a data package with multiple sets broken down by years, it will have a slug
# at the end of the URL that looks something like pad_mdba_sexe_edat-1
# Call this function with that as the argument for "datapackage_id"
# file_slug should be whatever string you want at the beginning of all your files. the default is to
# take the first four characters from the resource name, which is usually a year if it's a multi-year dataset.

def get_resource_ids(datapackage_id: str, max_retries=3, file_slug='') -> list:
    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'

    for attempt in range(1, max_retries + 1):
        print(f'Attempt {attempt} to retrieve resource IDs...')
        response = requests.get(url, params={'id': datapackage_id})

        if response.status_code == 200:
            data = response.json()
            resources = data['result']['resources']
            id_list = []
            for res in resources:
                if res['name'].endswith('.csv'):
                    if file_slug:
                        tag = file_slug
                    else:
                        tag = int(res['name'][0:4])
                    id_list.append((tag, res['id']))
            list_length = len(id_list)
            print(f'Successfully retrieved {list_length} resource IDs in {response.elapsed.total_seconds()} seconds.')
            return id_list
        else:
            print(f'Attempt failed with {response.status_code} code in {response.elapsed.total_seconds()} seconds.')
    print('Attempts to retrieve resource IDs were unsuccessful, try again later.')
    return []

# this function counts the rows before making the request so we know how high to set the limit
# when we query the actual data.
def count_rows(resource_id: tuple, max_retries=3) -> int:
    endpoint = 'https://opendata-ajuntament.barcelona.cat/data/api/3/action/datastore_search_sql'
    sql = f'SELECT COUNT(*) FROM \"{resource_id[1]}\"'
    params = {'sql': sql}
    for count_attempt in range(1, max_retries + 1):
        print(f'Querying the row count, attempt {count_attempt}...')
        count_response = requests.get(endpoint, params)
        if count_response.status_code != 200:
            print(f'The row count had a problem: {count_response.status_code}, took {count_response.elapsed.total_seconds()} seconds to respond.')
        else:
            count_data = count_response.json()
            row_count = int(count_data['result']['records'][0]['count'])
            print(f'Counted {row_count} rows in the target record, took {count_response.elapsed.total_seconds()} seconds to respond.')
            return row_count


def get_data(api_endpoint, resource_id: tuple, max_retries=3, backoff_factor=2) -> dict:

    rows = count_rows(resource_id)
    if not rows:
        print('The row count went wrong because the server is not responding. Try again later.')
        return {}

    for attempt in range(1, max_retries + 1):
        try:
            print(f'Making attempt number {attempt}...')
            limit = rows + 10
            response = requests.get(api_endpoint, params={
                'resource_id': resource_id[1],
                'limit': limit
            }
                                    )
            if response.status_code == 200:
                print(f'Success! API response time: {response.elapsed.total_seconds()} seconds')
                data = response.json()
                return data
            else:
                print(f'Attempt {attempt} failed: Experienced a {response.status_code} error.')
        except requests.RequestException as e:
            print(f'Attempt {attempt}: Request failed - {e}')

        if attempt < max_retries:
            sleep_time = backoff_factor ** attempt
            print(f'Retrying in {sleep_time} seconds...')
            time.sleep(sleep_time)

    print('Max retries reached. Giving up.')
    return {}

def save_to_csv(data: dict, directory: str, resource_tuple: tuple):
    if data and 'result' in data and 'records' in data['result']:
        count = 0
        with open(f'{directory}names_{resource_tuple[0]}.csv', 'w') as file:
            fieldnames = list(data['result']['records'][0].keys())
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for i in data['result']['records']:
                writer.writerow(i)
                count += 1
        print(f'Saved a CSV file with {count} entries!')
    else:
        print('No data available or invalid response structure.')
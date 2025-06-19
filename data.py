from io import StringIO
import logging, requests, os
import pandas as pd

def get_resource_library(
        logger: logging.Logger, 
        package_name: str
        ) -> list:
    """
    Gets the resource library for a particular package from Open Data BCN.

    Args: 
        logger (logging.Logger): A logging instance for recording events.
        package_name (str): Name of an Open Data BCN package containing multiple resources.

    Returns:
        resources (list): A list of resource dictionaries   
    """

    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'

    #TODO: add logging
    #TODO: add error handling 
    response = requests.get(url, params={'id': package_name})
    data = response.json()
    resources = data['result']['resources']
    csv_resources = []
    for res in resources:
        if res['name'].lower().endswith('.csv'):
            #The original resource dictionary doesn't include the package name, so this adds it to each resource dict
            res['package_name'] = package_name
            csv_resources.append(res)
    return csv_resources

def download(logger: logging.Logger, resource: dict) -> StringIO:

    """
    Downloads and returns a CSV file object from the Open Data BCN respository.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource.

    Returns:
        In-memory StringIO object of a CSV file. 
    """

    #TODO: add a check to make sure the 'url' resource exists.
    #TODO: add error handling and logging
    url = resource['url']
    response = requests.get(url)
    csv_data = StringIO(response.content.decode('utf-8'))
    return csv_data

def save_csv(logger: logging.Logger, 
             resource: dict, 
             csv: StringIO, 
             path: str="./"
             ) -> bool:
    """
    Saves a CSV in-memory object to disk.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary containing information about the resource.
        csv (StringIO): A StringIO object that is a CSV file in memory.
        path (str): Parameter provided by user indicating where to save file (default: root)

    Returns:
        A boolean operator indicating if the operation was successful or not.
    """
    csv.seek(0)
    #TODO: add logging
    final_path = os.path.join(
        path, 
        resource['package_name'], 
        resource['name']
        )
    
    #TODO: add try/except here to catch if user does not have permissions
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    try:
        with open(final_path, 'w', encoding='utf-8') as f:
            f.write(csv.getvalue())
        return True
    except Exception as e:
        print("There was a problem saving the file: {e}") #change this to a logger call
        return False

def to_df(logger: logging.Logger, resource: dict, csv: StringIO) -> pd.DataFrame:

    """
    Takes an in-memory CSV object and returns a pandas dataframe.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary containing information about the resource.
        csv (StringIO): A StringIO object that is a CSV file in memory.
    """
    
    #TODO: add logging
    csv.seek(0)
    return pd.read_csv(csv)

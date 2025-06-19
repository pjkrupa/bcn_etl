from io import StringIO
from typing import Tuple
import logging, requests, os
import pandas as pd

def get_resource_library(
        logger: logging.Logger, 
        package_name: str
        ) -> Tuple[list, int]:
    """
    Gets the resource library for a particular package from Open Data BCN.

    Args: 
        logger (logging.Logger): A logging instance for recording events.
        package_name (str): Name of an Open Data BCN package containing multiple resources.

    Returns:
        [0] A list of resource dictionaries
        [1] The time in seconds it took for the response
    """

    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'

    #TODO: add logging
    #TODO: add error handling 
    
    logger.info("-------------------------------------------")
    logger.info(f"Making GET request for list of resources in the {package_name} data package...")

    try:
        response = requests.get(url, params={'id': package_name})
    except Exception as e:
        logger.exception(e)
        logger.info("-------------------------------------------")
        return []

    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Seconds to response: {response.elapsed.seconds}')
    logger.info(f'Processing response...')

    data = response.json()
    resources = data['result']['resources']
    csv_resources = []
    for res in resources:
        if res['name'].lower().endswith('.csv'):
            #The original resource dictionary doesn't include the package name, so this adds it to each resource dict
            res['package_name'] = package_name
            csv_resources.append(res)

    logger.info(f'Successfully collected details on {len(csv_resources)} resources belonging to the {package_name} data package!')
    logger.info("-------------------------------------------")
    return csv_resources, response.elapsed.seconds


def download(logger: logging.Logger, resource: dict) -> Tuple[StringIO, int]:

    """
    Downloads and returns a CSV file object from the Open Data BCN respository.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource.

    Returns:
        [0] In-memory StringIO object of a CSV file.
        [1] The time in seconds it took for the response.
    """

    #TODO: add a check to make sure the 'url' resource exists.
    #TODO: add error handling and logging
    if not resource.get('url') or not resource['url']:
        logger.error(f'Sorry, this resource has no download URL available!')
        return
    
    url = resource['url']

    logger.info("-------------------------------------------")
    logger.info(f"Downloading file...")

    try:
        response = requests.get(url)

        logger.info(f'Response code: {response.status_code}')
        logger.info(f'Seconds to response: {response.elapsed.seconds}')
        logger.info(f'Processing response...')

        try:
            csv_data = StringIO(response.content.decode('utf-8'))

            logger.info(f"Successfully downloaded the CSV file!")
            logger.info("-------------------------------------------")
            return csv_data, response.elapsed.seconds
        
        except Exception as e:
            logger.exception(f"There was an error while converting the response into a CSV: {e}")
            logger.info("-------------------------------------------")
            return
    
    except Exception as e:
        logger.exception(f"There was a problem downloading the CSV file: {e}")
        logger.info("-------------------------------------------")
        return
    
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
    
    logger.info("-------------------------------------------")
    logger.info("Saving CSV to disk...")

    final_path = os.path.join(
        path, 
        resource['package_name'], 
        resource['name']
        )
    
    try:
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
    except PermissionError as e:
        logger.error(f"Sorry, you don't have permission to save the file to {final_path}: {e}")
        return False
    
    try:
        with open(final_path, 'w', encoding='utf-8') as f:
            f.write(csv.getvalue())
        logger.info(f"Succesfully saved CSV file to {final_path}!")
        logger.info("-------------------------------------------")
        return True
    except Exception as e:
        logger.error("There was a problem saving the file: {e}")
        logger.info("-------------------------------------------")
        return False


def to_df(logger: logging.Logger, resource: dict, csv: StringIO) -> pd.DataFrame:

    """
    Takes an in-memory CSV object and returns a pandas dataframe.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary containing information about the resource.
        csv (StringIO): A StringIO object that is a CSV file in memory.
    """
    
    csv.seek(0)
    
    logger.info("-------------------------------------------")
    logger.info(f"Converting to a dataframe...")
    
    try:
        df = pd.read_csv(csv)
        logger.info("Successfully converted CSV to a dataframe!")
        logger.info("-------------------------------------------")
        return df
    except Exception as e:
        logger.exception(f"Something went wrong: {e}")
        logger.info("-------------------------------------------")
        return

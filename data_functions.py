from io import StringIO
from typing import Optional
import logging, requests, os, csv
import pandas as pd


def request_resource_library(
        logger: logging.Logger, 
        package_name: str
        ) -> Optional[requests.Response]:
    """
    Requests the resource library for a particular package from Open Data BCN.

    Args: 
        logger (logging.Logger): A logging instance for recording events.
        package_name (str): Name of an Open Data BCN package containing multiple resources.

    Returns:
        The requests.Response object with all the info from the response
    """

    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'
    
    try:
        response = requests.get(url, params={'id': package_name}, timeout=10)
        return response
    
    except requests.exceptions.ConnectTimeout:
        logger.error(f"Connection to Open Data BCN timed out while requesting package details for '{package_name}'.")
    except requests.exceptions.Timeout:
        logger.error(f"Request for package details for '{package_name}' timed out.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch package '{package_name}': {e.__class__.__name__} - {e}")
        logger.debug("Full exception details:", exc_info=True)
        return None


def process_resource_library(
        logger: logging.Logger, 
        response: requests.Response,
        package: str,
        ) -> list[dict]:
    
    """
    Processes the request object to extract a package resource library into a list of dictionaries.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        response (requests.Response): A raw response object containing package information.
        package (str): The name of the package being processed.
    Returns:
        A list of dictionaries.
    """
    data = response.json()

    try:
        resources = data['result']['resources']
    except Exception as e:
        logger.exception(f"There was a problem accessing the resources from the request object: {e}")
        return None
    csv_resources = []
    for res in resources:
        if res['format'] == "CSV":
            #The original resource dictionary doesn't include the package name, so this adds it to each resource dict
            res['package_name'] = package
            csv_resources.append(res)
    return csv_resources


def token_required(logger: logging.Logger, resource: dict) -> bool:
    """
    Checks to see if resource requires a token for access.
    
    Args: 
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource.
    Returns: True if the resource requires a token, False if not.
    """
    if resource['token_required'].strip().lower() == 'yes':
        return True
    else:
        return False

def download_resource(logger: logging.Logger, resource: dict) -> Optional[requests.Response]:

    """
    Downloads a resource from the Open Data BCN respository as a requests.Response object.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource.

    Returns:
        A requests.Response object where ".content" is the data for the CSV file.
    """


    if not resource.get('url'):
        logger.error(f'Sorry, this resource has no download URL available!')
        return None
    
    url = resource['url']

    try:
        response = requests.get(url, timeout=10)        
        return response
    except requests.exceptions.ConnectTimeout:
        logger.error(f"Connection to Open Data BCN timed out while requesting {resource['name']}.")
    except requests.exceptions.Timeout:
        logger.error(f"Request to download '{resource['name']}' timed out.")
    except requests.RequestException as e:
        logger.exception(f"There was a problem downloading {resource['name']}: {e.__class__.__name__} - {e}")
        logger.debug("Full exception details:", exc_info=True)
        return None
    
    
def convert_to_csv(
        logger: logging.Logger,  
        response: requests.Response
        ) -> Optional[StringIO]:
    """
    Converts a response.Requests object to a CSV StringIO object.
    """
    try:
        try:
            decoded = response.content.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("UTF-8 decoding failed, retrying with UTF-16.")
            decoded = response.content.decode('utf-16')

        csv_data = StringIO(decoded)
        reader = csv.reader(csv_data)

        #simple check to see if the file is actually a CSV. if not, it throws an error.
        first_row = next(reader)
        
        logger.info(f"Successfully converted response object to a CSV file.")
        return csv_data

    except csv.Error as e:
        logger.excption(f"Response content is not valid CSV format: {e}")
        return None
    
    except StopIteration:
        logger.warning("CSV appears to be empty.")
        return None

    except Exception as e:
        logger.exception(f"There was an error while converting the response into a CSV: {e}")
        return None
    
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
    
    logger.info("Saving CSV to disk...")

    dir_path = os.path.join(
        path, 
        resource['package_name'],
        )
    
    try:
        os.makedirs(dir_path, exist_ok=True)
    except PermissionError as e:
        logger.error(f"Sorry, you don't have permission to create the directory {dir_path}: {e}")
        return False
    
    final_path = os.path.join(dir_path, resource['name'])

    try:
        with open(final_path, 'w', encoding='utf-8') as f:
            f.write(csv.getvalue())
        logger.info(f"Succesfully saved CSV file to {final_path}.")
        return True
    
    except Exception as e:
        logger.error(f"There was a problem saving the file: {e}")
        return False


# Going to use this function eventually to load CSVs into Postgres.
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

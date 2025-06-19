from io import StringIO
from typing import Tuple
import logging, requests, os, csv
import pandas as pd

#TODO: break this up into two functions, one that returns the request and another that returns the processed library.
def get_resource_library(
        logger: logging.Logger, 
        package_name: str
        ) -> Tuple[list, requests.Response]:
    """
    Gets the resource library for a particular package from Open Data BCN.

    Args: 
        logger (logging.Logger): A logging instance for recording events.
        package_name (str): Name of an Open Data BCN package containing multiple resources.

    Returns:
        [0] A list of resource dictionaries
        [1] The requests.Response object with all the info from the response
    """

    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/package_show'

    #TODO: add error handling 
    
    logger.info("-------------------------------------------")
    logger.info(f"Making GET request for list of resources in the {package_name} data package...")

    try:
        response = requests.get(url, params={'id': package_name})
    except Exception as e:
        logger.exception(e)
        return None

    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Seconds to response: {response.elapsed.seconds}')

    if response.status_code == 200:
        logger.info(f'Processing response...')

        data = response.json()
        resources = data['result']['resources']
        csv_resources = []
        for res in resources:
            if res['name'].lower().endswith('.csv'):
                #The original resource dictionary doesn't include the package name, so this adds it to each resource dict
                res['package_name'] = package_name
                csv_resources.append(res)

        logger.info(f'Successfully collected details on resources belonging to the {package_name} data package.')
        return csv_resources, response
    else:
        logger.error(f"Sorry, I couldn't retrieve the resources for the {package_name} package, got error code {response.status_code}.")
        return None, response


#TODO: add a check for whether a resource requires a token
def download(logger: logging.Logger, resource: dict) -> requests.Response:

    """
    Downloads a resource from the Open Data BCN respository as a requests.Response object.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource.

    Returns:
        A requests.Response object where ".content" is the data for the CSV file.
    """


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
    
    except Exception as e:
        logger.exception(f"There was a problem downloading the CSV file: {e}")
        return None
    
    if response.status_code == 200:
        logger.info(f'Success! Resource retrieved.')
        return response
    else:
        logger.error(f'Sorry, I got a {response.status_code} error and was not able to download this one.')
        return None
    
    
def convert_to_csv(
        logger: logging.Logger,  
        response: requests.Response
        ) -> StringIO:
    """
    Converts a response.Requests object to a CSV StringIO object.
    """
    try:
        csv_data = StringIO(response.content.decode('utf-8'))
        reader = csv.reader(csv_data)

        #simple check to see if the file is actually a CSV. if not, it throws an error.
        first_row = next(reader)
        
        logger.info(f"Successfully converted response object to a CSV file.")
        return csv_data
    
    except UnicodeDecodeError as e:
        logger.exception(f"Failed to decode response content as UTF-8: {e}")
        return None 

    except csv.Error as e:
        logger.excption(f"Response content is not valid CSV format: {e}")
        return None
    
    except StopIteration:
        logger.warning("CSV appears to be empty.")
        return None

    except Exception as e:
        logger.exception(f"There was an error while converting the response into a CSV: {e}")
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

import logging, requests, time
from typing import Optional
from data_functions import download_resource, request_resource_library

#TODO: implement backoff
def persistant_request(
        logger: logging.Logger,
        resource: dict = None,
        package: str = None,
        backoff_factor: int = 2,
        max_retries: int = 3,
        ) -> Optional[requests.Response]:
    
    """
    A function for making multiple tries at retrieving package info and resources.
    Works on requests for both packages and resources

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with information about the resource. If getting a package, leave None.
        package (str): Name of an Open Data BCN package containing multiple resources. If getting a resource, leave None.
        backoff_factor (int): Keeps the script from hammering the servers too much.
        max_retries (int): Maximum number of times the loop retries the request

    Returns:
        A request.Response object if a response is received, or None if no response is received.
    """

    attempts_remaining = max_retries

    while attempts_remaining > 0:
        if resource:
            response = download_resource(logger, resource)
        else:
            response = request_resource_library(logger, package)
            
            

        if response and response.status_code == 200:
            return response
        else:
            if response:
                status = response.status_code
            else:
                status = "No response."
            logger.error(f"Problem with response from server: {status}.")
            attempts_remaining -= 1

            if attempts_remaining > 0:
                wait_time = backoff_factor * (2 ** (max_retries - attempts_remaining))
                logger.info(f"Trying again in {wait_time} seconds...")
                time.sleep(wait_time)

        logger.error(f"Out of attempts.")
        return response
                 

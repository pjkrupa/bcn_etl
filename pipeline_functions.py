import logging, requests
from data_functions import download_resource, request_resource_library

def persistant_request(
        logger: logging.logger,
        resource: dict = None,
        package: str = None,
        backoff_factor: int = 2,
        max_retries: int = 3,
        ) -> requests.Response:
    
    """
    A function for making multiple tries at retrieving package info and resources
    Works on either packages or 
    """
    retry_list = []
    while max_retries != 0:
        if package:
            response = request_resource_library(logger, package)
        else:
            response = download_resource(logger, resource)
            

        if response and response.status_code == 200:
            return response
        else:
            if response:
                status = response.status_code
            else:
                status = "No response."
            logger.error(f"Problem with response from server: {status}.")
            max_retries -= 1
            if max_retries > 0:
                logger.info("Trying again...")
            else:
                logger.error(f"Out of attempts.")
                return response
        
            

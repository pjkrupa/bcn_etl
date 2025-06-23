import logging, requests, time, os
from typing import Optional
from data_functions import download_resource, request_resource_library, token_required, process_resource_library, convert_to_csv, save_csv
from reporting import Report

def persistant_request(
        logger: logging.Logger,
        report: Report,
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
        # This is to check if the function call is for a resource or a package.
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
            report.num_errors += 1
            attempts_remaining -= 1

            if attempts_remaining > 0:
                wait_time = backoff_factor * (2 ** (max_retries - attempts_remaining))
                logger.info(f"Trying again in {wait_time} seconds...")
                time.sleep(wait_time)

        logger.warning(f"Out of attempts.")
        return response
    
def main_pipeline(
        logger: logging.Logger, 
        package: str, 
        storage_root: str) -> dict:
    """
    This is the main pipeline for the script. 
    It takes a package name and then attempts to download all the resources in the package.
    It then returns a report for further processing in case of failures or errors.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        package (str): The name of a BCN Open Data package.
    
    Returns: 
        report (dict): Full report on the results.
    """
    start_time = time.time()
    report = Report(package, start_time)
    
    package.strip()
    logger.info("-------------------------------------------")
    logger.info(f"Sending GET request for list of resources in the {package} data package...")
    
    response = persistant_request(logger, package=package, report=report)
    
    if response is None:
        logger.error(f"Failed to access the {package} package's resource list, skipping to the next package...")
        report.num_errors += 1
        return report
    elif not response.status_code == 200:
        logger.error(f"Couldn't retrieve {package} package resources.")
        logger.error(f"Response code: {response.status_code}")
        report.num_errors += 1
        logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
        report.process_package_response(response)
        return report
    else:
        logger.info(f'Response code: {response.status_code}')
        logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
        report.process_package_response(response)

    resource_list = process_resource_library(logger, response, package)

    if not resource_list:
        logger.error(f"Failed to process this package's resource list, skipping to the next package...")
        report.num_errors += 1
        return report

    logger.info(f'Successfully collected details on resources belonging to the {package} data package.')
    report.package_success = True
    report.num_resources = len(resource_list)

    logger.info(f"This package contains a total of {len(resource_list)} resources.")
    logger.info("////////////////////////////////////////////////////////")

    # this is to check if the file has already been downloaded
    save_path = os.path.join(storage_root, package)
    try: 
        existing_downloads = os.listdir(save_path)
    except FileNotFoundError:
        existing_downloads = []
    for resource in resource_list:
        if resource['name'] not in existing_downloads:
            get_resource(logger, resource, report, storage_root)
        else:
            report.skipped += 1

    return report

def get_resource(
        logger: logging.Logger, 
        resource: dict, 
        report: Report, 
        storage_root: str
        ):
    """
    A pipeline function that downloads and saves a single CSV resource and updates the report for the package.

    Args:
        logger (logging.Logger): A logging instance for recording events.
        resource (dict): A dictionary with all the information on the resource.
        report (Report): A Report object to be updated as the function works.
        storage_root (str): the root directory where the downloaded CSV files will be saved. Default (from parser) is '.'.

    Returns:
        None, but its actions are recorded in the report object.
    """

    logger.info(f'Sending a request for {resource["name"]}.')
    if token_required(logger, resource) is True:
        logger.error(f"Sorry, a token is required to access {resource['name']}")
        report.num_errors += 1
        return
    
    response = persistant_request(
        logger, 
        report=report, 
        resource=resource
        )
    
    if response is None:
        logger.error(f"Failed to download {resource['name']}. Trying next resource...")
        report.num_errors += 1
        report.add_resources_fail(resource)
        return
    elif not response.status_code == 200:
        logger.error(f"Couldn't download {resource['name']}.")
        logger.error(f"Response code: {response.status_code}")
        logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
        report.num_errors += 1
        report.process_resource_response(response, resource)
        return
    else:
        logger.info(f'Response code: {response.status_code}')
        logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
        report.process_resource_response(response, resource)

    
    csv = convert_to_csv(logger, response)
    saved = save_csv(logger, resource, csv, path=storage_root)

    if saved:
        logger.info(f"{len(report.resources_success)} of {report.num_resources} resources collected.")
        logger.info('■'*len(report.resources_success))
    else:
        logger.error(f"Could not save {resource['name']} to disk.")
        report.num_errors += 1
                 

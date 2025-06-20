import time
from logging_setup import get_logger
from data_functions import request_resource_library, process_resource_library, download_resource, convert_to_csv, save_csv, to_df, token_required
from pipeline_functions import persistant_request


#TODO: set up argparse to handle command line parameters
#TODO: add backoff timer
#TODO: handle failed downloads in the main loop. check response codes, collect URLs of failed downloads, circle back and try again.

logger = get_logger()
package_list = ["pad_dom_mdbas_tipus-domicili_edat"]



if __name__ == "__main__":
    
    for package in package_list:
        logger.info("-------------------------------------------")
        logger.info(f"Sending GET request for list of resources in the {package} data package...")
        
        response = persistant_request(logger, package=package)
        
        if response is None:
            logger.error(f"Failed to access this package's resource list, skipping to the next package...")
            continue
        elif not response.status_code == 200:
            logger.error(f"Couldn't retrieve {package} package resources.")
            logger.error(f"Response code: {response.status_code}")
            logger.info(f'Seconds to response: {response.elapsed.seconds}')
            continue
        else:
            logger.info(f'Response code: {response.status_code}')
            logger.info(f'Seconds to response: {response.elapsed.seconds}')

        resource_list = process_resource_library(logger, response)

        if not resource_list:
            logger.error(f"Failed to process this package's resource list, skipping to the next package...")
            continue

        logger.info(f'Successfully collected details on resources belonging to the {package} data package.')

        start_time = time.time()
        resource_count = len(resource_list)
        resources_downloaded = 0

        logger.info(f"This package contains a total of {resource_count} resources.")
        logger.info(f"Downloading them now...")
        logger.info("////////////////////////////////////////////////////////")

        #TODO: add logic to retry several times with a backoff timer.
        fail_list = []
        for resource in resource_list:
            if token_required(resource) is True:
                logger.error(f"Sorry, a token is required to access {resource['name']}")
                continue
            response = persistant_request(logger, resource=resource)
            
            if not response.status_code == 200:
                fail_list.append(resource)
            else:
                csv = convert_to_csv(logger, response)
                saved = save_csv(logger, resource, csv)
                if saved:
                    resources_downloaded += 1
                    logger.info(f"{resources_downloaded} of {resource_count} resources collected.")
                    logger.info('■'*resources_downloaded)
        
        
        end_time = time.time()
        elapsed = end_time - start_time
        logger.info(f"All done with {package}. Processing all the resources in this package took {elapsed:.2f} seconds.")
        logger.info("////////////////////////////////////////////////////////")





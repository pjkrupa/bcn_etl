import time
from logging_setup import get_logger
from data_functions import request_resource_library, process_resource_library, download_resource, convert_to_csv, save_csv, to_df, token_required
from pipeline_functions import persistant_request


#TODO: set up argparse to handle command line parameters
#TODO: add backoff timer
#TODO: handle failed downloads in the main loop. check response codes, collect URLs of failed downloads, circle back and try again.

logger = get_logger()
package_list = ["pad-dimensions", "pad_cdo_b_barri-des", "pad_dom_mdbas_dones", "pad_dom_mdbas_edat-0018"]

#"pad_dom_mdbas_tipus-domicili_edat"


#TODO: Produce a report at the end of the pipeline.
if __name__ == "__main__":
    
    # Start of the loop collecting information on data packages in the list:
    for package in package_list:
        logger.info("-------------------------------------------")
        logger.info(f"Sending GET request for list of resources in the {package} data package...")
        
        response = persistant_request(logger, package=package)
        
        if response is None:
            logger.error(f"Failed to access the {package} package's resource list, skipping to the next package...")
            continue
        elif not response.status_code == 200:
            logger.error(f"Couldn't retrieve {package} package resources.")
            logger.error(f"Response code: {response.status_code}")
            logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
            continue
        else:
            logger.info(f'Response code: {response.status_code}')
            logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')

        resource_list = process_resource_library(logger, response, package)

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

        # Start of the loop downloading the package's resources:
        fail_list = []
        for resource in resource_list:
            logger.info(f'Sending a request for {resource["name"]}.')
            if token_required(logger, resource) is True:
                logger.warning(f"Sorry, a token is required to access {resource['name']}")
                continue
            response = persistant_request(logger, resource=resource)
            
            if response is None:
                logger.error(f"Failed to download {resource['name']}. Trying next resource...")
                continue
            elif not response.status_code == 200:
                logger.error(f"Couldn't download {resource['name']}.")
                logger.error(f"Response code: {response.status_code}")
                logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')
                continue
            else:
                logger.info(f'Response code: {response.status_code}')
                logger.info(f'Seconds to response: {response.elapsed.total_seconds():.2f}')

            if not response.status_code == 200:
                fail_list.append(resource)
            
            csv = convert_to_csv(logger, response)
            saved = save_csv(logger, resource, csv)

            if saved:
                resources_downloaded += 1
                logger.info(f"{resources_downloaded} of {resource_count} resources collected.")
                logger.info('■'*resources_downloaded)
            else:
                fail_list.append(resource["name"])
        
        if fail_list:
            logger.warning(f'The following resources for the package {package} failed to download:')
            for res in fail_list:
                logger.warning(f"{res}")

        end_time = time.time()
        elapsed = end_time - start_time
        logger.info(f"All done with {package}. Processing all the resources in this package took {elapsed:.2f} seconds.")
        logger.info("////////////////////////////////////////////////////////")





import time
from logging_setup import get_logger
from data_functions import get_resource_library, download, convert_to_csv, save_csv, to_df


#TODO: set up argparse to handle command line parameters
#TODO: add backoff timer
#TODO: track how many resources were downloaded for each package to ensure they were all collected
#TODO: handle failed downloads in the main loop. check response codes, collect URLs of failed downloads, circle back and try again.

logger = get_logger()
package_list = ["pad_dom_mdbas_tipus-domicili_edat"]



if __name__ == "__main__":
    
    for package in package_list:
        resource_list, response = get_resource_library(logger, package)
        if resource_list and response.status_code == 200:
            start_time = time.time()
            resource_count = len(resource_list)
            resources_downloaded = 0

            logger.info(f"Finished getting resource list, this package contains a total of {resource_count} resources.")
            logger.info("////////////////////////////////////////////////////////")

            logger.info(f"Downloading resources...")

            #TODO: add logic to retry several times with a backoff timer.
            for resource in resource_list:
                fail_list = []
                response = download(logger, resource)
                if response.status_code != 200:
                    fail_list.append(resource)
                else:
                    csv = convert_to_csv(logger, response)
                    saved = save_csv(logger, resource, csv)
                    if saved:
                        resources_downloaded += 1
                        logger.info(f"{resources_downloaded} of {resource_count} resources collected.")
            end_time = time.time()
            elapsed = end_time - start_time
            logger.info(f"All done with {package}. Downloading this package took {elapsed:.2f} seconds.")
            logger.info("////////////////////////////////////////////////////////")





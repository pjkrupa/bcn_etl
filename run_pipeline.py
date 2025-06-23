import time
from datetime import timedelta
from logging_setup import get_logger
from parser_setup import get_parser
from data_functions import request_resource_library, process_resource_library, download_resource, convert_to_csv, save_csv, to_df, token_required
from pipeline_functions import persistant_request, main_pipeline
from reporting import compile_reports

#TODO: add logic to check if a resource has already been downloaded
#TODO: make the time logging system more precise, I think it's off
#TODO: add a README.md
#TODO: pull out logging so not so much ends up in the terminal
#TODO: make it so a different log is kept for each run
#TODO: do some work on the Report class, i think there's too much stuff going on

#package_list = ["pad-dimensions", "pad_cdo_b_barri-des", "pad_dom_mdbas_dones", "pad_dom_mdbas_edat-0018"]
# pad-dimensions pad_cdo_b_barri-des pad_dom_mdbas_dones pad_dom_mdbas_edat-0018
#"pad_dom_mdbas_tipus-domicili_edat"

### 
# Terminology note: Barcelona's Open Data repository uses the [INSERT HERE] standard. That means it's structured into
# "packages" that contain a series of "resources" (basically, annual datasets). When this script is accessing "packages",
# what it is doing is retrieving a list of the "resources" that belong to each "package".
# 
# So, when this code uses the term "package", it is referring to a collection of datasets, and when it uses the term
# "resource", it is referring to a dictionary containing information about an individual dataset, including the URL 
# for downloading the dataset in the form of a CSV.

parser = get_parser()
args = parser.parse_args()
package_list = args.packages
storage_root = args.directory
print(package_list)


if __name__ == "__main__":
    logger = get_logger()
    start_time = time.time()
    report_list = []
    for package in package_list:
        report = main_pipeline(logger, package, storage_root=storage_root)
        report_list.append(report)
    
    end_time = time.time()

    total_duration = end_time - start_time
    final_duration = str(timedelta(seconds=round(total_duration)))

    final_report = compile_reports(report_list)

    total_packages = len(final_report['packages_success']) + len(final_report['packages_fail'])
    total_resources = len(final_report['resources_success']) + len(final_report['resources_fail'])

    logger.info("***************************************************")
    logger.info("FINAL REPORT")
    logger.info(f"This pipeline ran for {final_duration}.")
    logger.info(f"A total of {len(final_report['packages_success'])} package(s) accessed successfully, out of {total_packages} attempted.")
    logger.info(f"A total of {len(final_report['resources_success'])} resource(s) downloaded and saved successfully, out of {total_resources} attempted.")
    logger.info(f"A total of {final_report['skipped']} resources skipped because they were already downloaded.")
    if final_report['packages_fail']:
        logger.info(f"The following package(s) could not be accessed:")
        for package in final_report['packages_fail']:
            logger.info(f"{package}")
        logger.info(f"Check the logs for more information.")

    if final_report['resources_fail']:
        logger.info(f"The following resource(s) could not be accessed:")
        for resource in final_report['resources_fail']:
            logger.info(f"{resource['name']}")
        logger.info(f"Check the logs for more information.")




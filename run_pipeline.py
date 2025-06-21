import time
from datetime import timedelta
from logging_setup import get_logger
from parser_setup import get_parser
from data_functions import request_resource_library, process_resource_library, download_resource, convert_to_csv, save_csv, to_df, token_required
from pipeline_functions import persistant_request, main_pipeline
from reporting import compile_reports


#TODO: set up argparse to handle command line parameters





#package_list = ["pad-dimensions", "pad_cdo_b_barri-des", "pad_dom_mdbas_dones", "pad_dom_mdbas_edat-0018"]

#"pad_dom_mdbas_tipus-domicili_edat"
parser = get_parser()
args = parser.parse_args()
package_list = args.packages
directory = args.directory
print(package_list)


#TODO: Save a separate report/logs for each run.
if __name__ == "__main__":
    logger = get_logger()
    start_time = time.time()
    report_list = []
    for package in package_list:
        report = main_pipeline(logger, package)
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




import time

def compile_reports(report_list: list):
    final_report = {
        'packages_success': [],
        'packages_fail': [],
        'resources_success': [],
        'resources_fail': [],
        'total_duration': 0,
        'num_errors': 0,
    }

    for report in report_list:
        final_report['num_errors'] += report.num_errors
        final_report['total_duration'] = report.end_time - report.start_time
        if not report.package_success:
            final_report['packages_fail'].append(report.package_name)
            continue
        final_report['packages_success'].append(report.package_name)
        final_report['resources_success'].extend(report.resources_success)
        final_report['resources_fail'].extend(report.resources_fail)
    return final_report




class Report:

    def __init__(self, package: str, start_time: time.time):
        self.package_name = package
        self.package_success = False
        self.package_response_code = None
        self.num_resources = 0
        self.resources_success = []
        self.resources_fail = []
        self.total_duration = 0
        self.start_time = 0
        self.end_time = 0
        self.num_errors = 0

    def process_package_response(self, response):
        self.total_duration += response.elapsed.total_seconds()
        self.package_response_code = response.status_code
    
    def process_resource_response(self, response, resource):
        self.total_duration += response.elapsed.total_seconds()
        if not response.status_code == 200:
            self.resources_fail.append(resource)
        else:
            self.resources_success.append(resource['name'])
    
    def add_resources_fail(self, resource):
        self.resources_fail.append(resource)

    def add_to_total_duration(self, seconds):
        self.total_duration += seconds
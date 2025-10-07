# bcn_etl
A simple little tool for downloading datasets from Barcelona's Open Data BCN platform.

- [Intro](#intro)
- [Installation](#installation)
- [Running the Script](#running-the-script)
- [Logging](#logging)
- [Future Development](#future-development)


## Intro
I was looking at doing a data analysis project using datasets from Barcelona's Open Data BCN platform, but getting the data was quite challenging. The site navigation is slow, downloading through the browser is cumbersome, and the site experiences frequent outages. However, because Open Data BCN uses the CKAN data management system, its API is well documented and easy to navigate. I figured having a reusable script for grabbing data programmatically from Open Data BCN would be nice, so here it is.

Please note that this script is kind of a big gun. It's for when you want to download **all** the resources in a package or series of packages. 

## Installation

These instructions are for Linux. Some adjustments may be necessary for MacOS or WSL. 

This project uses [`uv`](https://docs.astral.sh/uv/getting-started/installation/) for vitual environment and dependency management.

Clone the repository from GitHub:

```bash
git clone https://github.com/pjkrupa/bcn_etl
```

Then navigate to the `bcn_etl` directory and run the following:

```bash
uv venv
source .venv/bin/activate
uv sync
```

## Running the script

Open Data BCN organizes its catalog as datasets (called "packages" here), each containing one or more downloadable CSV files ("resources").

To run the script, you need one or more package names from Open Data BCN. You can get package names from the data catalog, which is available at: [https://opendata-ajuntament.barcelona.cat/data/en/dataset/cataleg-opendata](https://opendata-ajuntament.barcelona.cat/data/en/dataset/cataleg-opendata)

The syntax for running the script from the `bcn_etl` directory is:

```bash
python3 run_pipeline.py (-p package-names | -t tags) -d your/download/directory --to_db
```
- The `-p` (or `--packages`) parameter can be one or many package names separated by a space.
- The `-t` (or `--tags`) parameter allows you to download all BCN packages with certain tags (list of all tags [here](https://opendata-ajuntament.barcelona.cat/data/ca/tags).) The search is inclusive: If a package has at least one tag, it will be downloaded.
- The `-d` (or `--directory`) parameter is optional: If you leave it off, everything will be saved in the `bcn_etl` directory.
- The `--to_db` flag is optional. If set, the packages will immediately be saved to the database based on the values in the `.env` file.

So running the script could look like this:

`python3 run_pipeline.py -p pad-dimensions pad_cdo_b_barri-des pad_dom_mdbas_dones -d csv_files`

Or like this:

`python3 run_pipeline.py -t transporte -d csv_files --to_db`

The script creates a directory for each package in `~/bcn_etl/csv_files` and downloads and saves all the package's resources there as CSV files. It skips resources that have been previously downloaded. When the script is finished, if the servers were up and everything worked, it should produce a report at the end that looks like this:
```
17:46:11 - INFO - FINAL REPORT
17:46:11 - INFO - This pipeline ran for 0:00:59.
17:46:11 - INFO - A total of 3 package(s) accessed successfully, out of 3 attempted.
17:46:11 - INFO - A total of 58 resource(s) downloaded and saved successfully, out of 58 attempted.
17:46:11 - INFO - A total of 0 resources skipped because they were already downloaded.
17:46:11 - INFO - There were 0 errors.
```

## Logging

The script displays logs in the terminal while it is running, but also saves them in the `bcn_etl` directory to `etl.log`, if you want to go back and audit what happened. It appends new logs to the existing file, so if you've run the pipeline a few times and want to start your logging fresh, just delete or rename `etl.log`.

## Future Development

I have a lot of little features and tweaks I still need to add to this:
- I need to add testing, something that's becoming pressing as the code gets more complex
- I want to break down the logging to make it both less verbose in the terminal and more detailed in the `etl.log` file. 
- I also want to add an option to make the script persistent, running in the background and sending repeated requests for when the Open Data BCN platform is being finicky. 
- Lastly, a bigger thing I want to do is add the option to load the downloaded data directly into a PostgreSQL database instead of saving it to a CSV.

## Contributing
Feel free to open issues or pull requests if you find a bug or want to suggest improvements.




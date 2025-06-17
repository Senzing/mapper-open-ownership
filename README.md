# mapper-open-ownership

## Overview

The [oor_mapper.py] python script converts the Open Ownership Register to json files ready to load into Senzing.

The Open Ownership download page is [here]. Just select the latest date and download the file.
It should be named something like "statements.yyyy-mm-ddThh_mm_ssZ.jsonl.gz"

Usage:

```console
python oor_mapper.py --help
usage: oor_mapper.py [-h] [-i INPUT_PATH] [-o OUTPUT_FILE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

## Contents

1. [Prerequisites]
2. [Installation]
3. [Configuring Senzing]
4. [Running the mapper]
5. [Loading into Senzing]

### Prerequisites

- python 3.6 or higher
- Senzing API version 3.1 or higher

### Installation

Place the the following files on a directory of your choice ...

- [oor_mapper.py]
- [oor_config_updates.g2c]

### Configuring Senzing

_Note:_ This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

Loading the Open Ownership Register into Senzing only requires registering the data source. No additional features or attributes are
required. This configuration is contained in the [oor_config_updates.g2c] file.
To apply it, from your Senzing project's python directory type ...

```console
python3 G2ConfigTool.py <path-to-file>/oor_config_updates.g2c
```

### Running the mapper

Download the Open Ownership Register file from [https://register.openownership.org/download].
Just select the latest date and download the file. It should be named something like "statements.yyyy-mm-ddThh_mm_ssZ.jsonl.gz"

Then in a terminal session, navigate to where you downloaded this mapper and type ...

```console
python3 oor_mapper.py -i /download_path/statements.yyyy-mm-ddThh_mm_ssZ.jsonl.gz -o /output_path/sz_oor_register.yyyy-mm-dd.jsonl.gz
```

- If the output file name ends with ".gz", the output file will be compressed
- Add the -l --log_file argument to generate a mapping statistics file

### Loading into Senzing

If you use the G2Loader program to load your data, from the /opt/senzing/g2/python directory ...

```console
python3 G2Loader.py -f /output_path/sz_oor_register.yyyy-mm-dd.jsonl.gz
```

This data set currently contains about 18 million entities and owners and make take several hours to load based on your hardware.

[oor_mapper.py]: oor_mapper.py
[here]: https://register.openownership.org/download
[Prerequisites]: #prerequisites
[Installation]: #installation
[Configuring Senzing]: #configuring-senzing
[Running the mapper]: #running-the-mapper
[Loading into Senzing]: #loading-into-senzing
[oor_config_updates.g2c]: oor_config_updates.g2c
[https://register.openownership.org/download]: https://register.openownership.org/download

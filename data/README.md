# Data Directory

This directory contains the data files required for the Scala Spark Broadcast Join project.

## Prerequisites

Before running any other commands in this project, you **must** download the required data files to this directory.

## Download Required Data Files

Execute the following commands from the project root directory to download the required data files:

```bash
# Download airline data
curl -O https://storage.googleapis.com/scala_broadcast_join/data/airline.csv

# Download carriers data
curl -O https://storage.googleapis.com/scala_broadcast_join/data/carriers.csv
```

## Data Files Description

- **airline.csv**: Contains airline flight data for analysis
- **carriers.csv**: Contains carrier information for joining with airline data

## Verification

After downloading, you should have the following files in this directory:
- `airline.csv`
- `carriers.csv`

## Next Steps

Once the data files are downloaded, you can proceed with running the project commands from the root directory.

## Note

- Ensure you have sufficient disk space for the data files
- The download process may take a few minutes depending on your internet connection
- If the download fails, please check your internet connection and try again 
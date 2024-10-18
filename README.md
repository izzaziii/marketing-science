# marketing-science
A toolkit of reusable Python scripts for marketing data analysis and reporting. Designed to streamline workflows from data processing to visualization, with a modular structure for flexible, efficient, and maintainable analytics.

- [marketing-science](#marketing-science)
  - [Data Retrieval via `datasets`](#data-retrieval-via-datasets)
    - [Setup](#setup)
    - [Configuration](#configuration)
      - [Meta Ads](#meta-ads)
    - [Scripts](#scripts)
      - [facebookads.py](#facebookadspy)
      - [boreport.py](#boreportpy)
  - [Authentication Handling With `auth`](#authentication-handling-with-auth)
      - [meta.py](#metapy)


## Data Retrieval via `datasets`

### Setup

1. Clone the repository

``` pwsh
git clone https://github.com/izzaziii/marketing-science.git
```

2. Setup venv
``` pwsh
python -m venv .venv
```

3. Install requirements
``` pwsh
# To add later
```

### Configuration

#### Meta Ads

In `/src/auth/config/meta_secrets.json`, include:

``` json
{
  "app_id": "your_app_id",
  "app_secret": "your_app_secret",
  "access_token": "your_access_token",
  "last_updated": "YYYY-MM-DD HH:MM"
}
```


### Scripts

#### facebookads.py
Fetches insights from Facebook Ads API and exports them as JSON.

**Run**: 
``` pwsh
python .\src\datasets\facebookads.py
```
**Inputs**: Date range (`start_date`, `end_date`) and access token (auto-loaded from `meta_secrets.json`).

**Output**: JSON file with insights data.

**Usage Example**:

1. Run `auth/meta.py` to refresh your access_token, if needed.
2. Use `facebookads.py` to pull data from a selected date range. Find the exported json file in `data/raw` folder.


#### boreport.py
This Python script processes sales data for weekly analysis and management updates, focusing on direct and all channels.

A VPN Connection to FortiClient is needed to access the shared folder where the BO Reports are located.

**Features**:
**Data Preparation**: Cleans and filters sales data for new acquisitions, focusing on specific sales channels and product lines.
**Resampling**: Resamples sales data weekly, grouped by key metrics like channel, bandwidth, state, and contract period.
**Usage**: To run the script, provide the file path to the sales data Excel file and call the main() function.

## Authentication Handling With `auth`

This module facilitates authentication for dealing with APIs involved in this repo. It includes meta.py for managing Facebook Ads API access tokens.

#### meta.py
Manages the Facebook API `access token`.

**Run**:
``` pwsh
python .\src\datasets\facebookads.py
```

**Input** : Short-lived token from [Facebook Graph API Explorer](https://developers.facebook.com/tools/explorer/)

**Output**: Saves a long-lived token and last updated timestamp in `\auth\config\meta_secrets.json`.


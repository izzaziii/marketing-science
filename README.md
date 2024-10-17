# marketing-science
A toolkit of reusable Python scripts for marketing data analysis and reporting. Designed to streamline workflows from data processing to visualization, with a modular structure for flexible, efficient, and maintainable analytics.

## Data Retrieval via `datasets`

This module automates retrieving insights data from Facebook Ads and exports it in JSON format. It includes two main scripts: facebookads.py for data retrieval and export, and ...

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


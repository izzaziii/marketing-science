# marketing-science
A toolkit of reusable Python scripts for marketing data analysis and reporting. Designed to streamline workflows from data processing to visualization, with a modular structure for flexible, efficient, and maintainable analytics.

- [marketing-science](#marketing-science)
  - [Setup](#setup)
  - [Folder Descriptions](#folder-descriptions)
      - [Meta Ads](#meta-ads)

## Setup

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
pip install -r requirements.txt
```

## Folder Descriptions
1. data
   1. Contains three subfolders: raw, interim, and processed.
   2. Such structure gives a framework on how to think about data in the workflow.
   3. Allows for a simple method of versioning.
2. notebooks
   1. All jupyter notebooks to be placed here.
   2. Make use of modules and functions in `src` to speed up work.
   3. Naming convention: {version}_{datetime}_{author}.ipynb
3. references
   1. Insert notes and other resource materials here
   2. Documentation to be here.
   3. Create subfolders as needed to maintain organized.
4. reports
   1. Final deliverables.
   2. Visualizations to be kept separate.
5. src
   1. Analysis
      1. Contains frequently-used code like resampling or plots
   2. Auth
      1. Authentication for data behind gateways
   3. databases
      1. CRUD operations for databases
   4. datasets
      1. Code to generate data
   5. utilities
      1. Frequently used code in areas of responsibility, not projects

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
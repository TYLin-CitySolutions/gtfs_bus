# gtfs_bus

Set-up
- clone repo
- create .venv
- .venv\Scripts\Activate
- python pip install -r requirements.txt

Process:
Local actions
- dowload GTFS static dataset from agency as .zip file
- update feeds.yml file with the new gfts data: id name, source type, and url location
- run ingest_gtfs.py file
- push to github
On github
- Combine_publish GitHub Action runs on push to main
- new data is added to main parquet files

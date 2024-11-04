# Leehwthgirb-Emohekat-Weivretni


## Requirements

Apache Spark >= 3.5.3

## Instructions

Add the data files to the root directory, next to main.py. They are expected to be named `source1.csv`, `source2.csv`, and `source3.csv`

In your virtual environment:
```
pip install pyspark
- or -
pip install -r requirements.txt
```

To run the program:
```
python main.py
```

## Notes

This is a simple script to handle the three source files for this takehome. There are several shortcomings born from time the time constraint that I will discuss here, in no particular order.

I am loading the files locally, which is not ideal. Preferably, we would be fetching files from a datastore or warehouse, like S3 or perhaps Snowflake. Fetching from an external source is more extensible and the code could be rewritten to pull from a folder, agnostic to file name or number of files, and it could take a cli variable to grab from different folders.

If I were writing for produciton, I would also like to separate the process for each source file, so that they can ran, be tracked, and updated separately. This would make it easier to extend to more sources without complicating other source ETLs.

Of course, rather than running this locally, it would be better to use a service like Databricks.

My QA is limited: I'm casting the column types, but a more thorough process would explore and test the data.

I had to make multiple judgement calls when working with this data that I would normally flag to the team, users, providers, or even turn to documentation to find answers. I created a custom string format for the 'ages_served' column, and I made several assumptions when interpreted said age data.

I created a single additional column to the required columns named 'source'. I find tracking the source of data can help when debugging strange issues, but it does come at a storage cost.

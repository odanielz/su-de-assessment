# config.py

BIGQUERY_PROJECT_ID = "ethereal-zodiac-443519-v8"
BIGQUERY_DATASET_NAME = "su_de_assessment"
TEMP_GCS_BUCKET = "sr-etl-temp-bucket"

CSV_TO_TABLE_MAPPING = {
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\tag.csv": "movie_tags",
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\rating.csv": "movie_ratings",
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\link.csv": "movie_links",
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\movie.csv": "movies",
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\genome_tags.csv": "genome_tags",
    r"C:\Users\O.Danielz\Documents\su-de-assessment\archive\genome_scores.csv": "genome_scores",
}

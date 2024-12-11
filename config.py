# config.py

BIGQUERY_PROJECT_ID = "ethereal-zodiac-443519-v8"
BIGQUERY_DATASET_NAME = "su_de_assessment"
TEMP_GCS_BUCKET = "sr-etl-temp-bucket"

CSV_TO_TABLE_MAPPING = {
    r"/Users/samsonakporotu/SparkingFlow/archive/tag.csv": "sr_de_assessment.movie_tags",
    r"/Users/samsonakporotu/SparkingFlow/archive/rating.csv": "sr_de_assessment.movie_ratings",
    r"/Users/samsonakporotu/SparkingFlow/archive/link.csv": "sr_de_assessment.movie_links",
    r"/Users/samsonakporotu/SparkingFlow/archive/movie.csv": "sr_de_assessment.movies",
    r"/Users/samsonakporotu/SparkingFlow/archive/genome_tags.csv": "sr_de_assessment.genome_tags",
    r"/Users/samsonakporotu/SparkingFlow/archive/genome_scores.csv": "sr_de_assessment.genome_scores",
}

POSTGRES_URL = "jdbc:postgresql://localhost:5433/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "samzy"
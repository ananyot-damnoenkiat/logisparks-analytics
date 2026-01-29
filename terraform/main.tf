# terraform/main.tf

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("../google_credentials.json")
  project     = "logisparks-analytics"
  region      = "asia-southeast3"
}

# 1. Create GCS Bucket (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = "logisparks-data-lake-ananyot"
  location      = "asia-southeast3"
  force_destroy = true
}

# 2. Create BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "logisparks_warehouse"
  location   = "asia-southeast3"
}
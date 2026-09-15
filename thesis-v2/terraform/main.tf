
data "google_compute_network" "kafka_vpc" {
  name = var.existing_vpc_name
}

resource "google_compute_subnetwork" "serverless" {
  name          = "kafka-serverless-subnet"
  ip_cidr_range = var.serverless_subnet_cidr
  region        = var.region
  network       = data.google_compute_network.kafka_vpc.id
}

resource "google_compute_firewall" "allow_serverless_to_kafka" {
  name    = "allow-serverless-to-kafka"
  network = data.google_compute_network.kafka_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_ranges = [var.serverless_subnet_cidr]
  target_tags   = ["kafka"]
}

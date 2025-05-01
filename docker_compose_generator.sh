#!/bin/bash

# Default values
CLIENTS=3
OUTPUT="docker-compose_test.yaml"
FILTER_BY_YEAR=2
FILTER_BY_COUNTRY=2
JOIN_CREDITS=2
JOIN_RATINGS=2
COUNT=4
SENTIMENT_ANALYSIS=2
AVERAGE_MOVIES_BY_RATING=1
MAX_MIN=1
TOP=1
AVERAGE_SENTIMENT=2
COLLECTOR_MAX_MIN=1
COLLECTOR_TOP_10_ACTORS=1
COLLECTOR_AVERAGE_SENTIMENT=1
INCLUDE_SENTIMENT_ANALYSIS=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --clients)
      CLIENTS="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --filter-by-year)
      FILTER_BY_YEAR="$2"
      shift 2
      ;;
    --filter-by-country)
      FILTER_BY_COUNTRY="$2"
      shift 2
      ;;
    --join-credits)
      JOIN_CREDITS="$2"
      shift 2
      ;;
    --join-ratings)
      JOIN_RATINGS="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
      shift 2
      ;;
    --sentiment-analysis)
      SENTIMENT_ANALYSIS="$2"
      shift 2
      ;;
    --average-movies-by-rating)
      AVERAGE_MOVIES_BY_RATING="$2"
      shift 2
      ;;
    --max-min)
      MAX_MIN="$2"
      shift 2
      ;;
    --top)
      TOP="$2"
      shift 2
      ;;
    --average-sentiment)
      AVERAGE_SENTIMENT="$2"
      shift 2
      ;;
    --collector-max-min)
      COLLECTOR_MAX_MIN="$2"
      shift 2
      ;;
    --collector-top-10-actors)
      COLLECTOR_TOP_10_ACTORS="$2"
      shift 2
      ;;
        
    --include-sentiment-analysis)
      INCLUDE_SENTIMENT_ANALYSIS=true
      shift
      ;;
    --collector-average-sentiment)
      COLLECTOR_AVERAGE_SENTIMENT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if python3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required but not installed"
    exit 1
fi

# Check if pyyaml is installed
python3 -c "import yaml" &> /dev/null
if [ $? -ne 0 ]; then
    echo "Installing PyYAML..."
    pip3 install pyyaml
fi

SENTIMENT_ARGS=""
if [ "$INCLUDE_SENTIMENT_ANALYSIS" = true ]; then
  SENTIMENT_ARGS="--include-sentiment-analysis"
fi

# Execute the Python script
python3 docker_compose_generator.py \
  --clients "${CLIENTS}" \
  --output "${OUTPUT}" \
  --filter-by-year "${FILTER_BY_YEAR}" \
  --filter-by-country "${FILTER_BY_COUNTRY}" \
  --join-credits "${JOIN_CREDITS}" \
  --join-ratings "${JOIN_RATINGS}" \
  --count "${COUNT}" \
  --sentiment-analysis "${SENTIMENT_ANALYSIS}" \
  --average-movies-by-rating "${AVERAGE_MOVIES_BY_RATING}" \
  --max-min "${MAX_MIN}" \
  --top "${TOP}" \
  --average-sentiment "${AVERAGE_SENTIMENT}" \
  --collector-max-min "${COLLECTOR_MAX_MIN}" \
  --collector-top-10-actors "${COLLECTOR_TOP_10_ACTORS}" \
  --collector-average-sentiment "${COLLECTOR_AVERAGE_SENTIMENT}" \
  ${SENTIMENT_ARGS}

echo "Docker Compose configuration generated successfully!"
echo "You can run it with: docker compose -f ${OUTPUT} up --build"
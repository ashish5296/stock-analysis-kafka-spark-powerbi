from newsapi import NewsApiClient
from kafka import KafkaProducer
from datetime import datetime
from textblob import TextBlob
import json

# API key for News API
news_api_key = "8b331c71e2294e45bfae1d93054177df"

# Initialize News API client
newsapi = NewsApiClient(api_key=news_api_key)

# Kafka Producer for news data
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Parameters
query = "Nvidia"
start_date = "2025-05-01"
end_date = "2025-05-25"

# Fetch news articles
news_articles = newsapi.get_everything(
    q=query,
    from_param=start_date,
    to=end_date,
    language="en",
    sort_by="relevancy"
)

# Process and send articles
for article in news_articles['articles']:
    published_date = datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d')
    message = {
        "date": published_date,
        "source": article['source']['name'],
        "title": article['title'],
        "description": article['description'] or "No description provided",
        "polarity": TextBlob(article['description'] or "").sentiment.polarity # Polarity ranges from -1 (negative) to 1 (positive)
    }
    producer.send('news-topic', value=message)
    print(f"Sent to Kafka (news-topic): {message}")

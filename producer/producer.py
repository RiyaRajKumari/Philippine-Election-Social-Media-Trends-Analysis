import json
import time
from kafka import KafkaProducer


KAFKA_BROKER = "18.61.53.176:9092"
TOPIC_NAME = "twitter"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


posts = [
    {
        "user": "user_001",
        "text": "Latest updates from the Philippine elections #Election2025 #Philippines",
        "hashtags": ["#Election2025", "#Philippines"],
        "timestamp": "2025-01-01T10:00:00",
    },
    {
        "user": "user_002",
        "text": "Following today's election news #Election2025 #News",
        "hashtags": ["#Election2025", "#News"],
        "timestamp": "2025-01-01T10:01:00",
    },
    {
        "user": "user_003",
        "text": "Important political discussion happening today #Philippines #Politics",
        "hashtags": ["#Philippines", "#Politics"],
        "timestamp": "2025-01-01T10:02:00",
    },
]


print("Sending social media posts...")

for index, post in enumerate(posts, start=1):

    producer.send(
        TOPIC_NAME,
        value=post,
    )

    print(
        f"Sent {index}/{len(posts)} | "
        f"user={post['user']} | "
        f"hashtags={post['hashtags']}"
    )

    time.sleep(1)


producer.flush()
producer.close()

print("Done sending messages.")

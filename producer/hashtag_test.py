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
        "user": "35681769520295",
        "text": "Election discussion #EDSA",
        "hashtags": ["#EDSA"],
        "timestamp": "2025-01-01T10:00:00",
    },
    {
        "user": "410399243094530",
        "text": "Live updates #PBBCollab5thEvictionNight",
        "hashtags": ["#PBBCollab5thEvictionNight"],
        "timestamp": "2025-01-01T10:01:00",
    },
    {
        "user": "589029215591253",
        "text": "Trending discussion #MissWorld2025",
        "hashtags": ["#MissWorld2025"],
        "timestamp": "2025-01-01T10:02:00",
    },
    {
        "user": "660427660526578",
        "text": "Political discussion #SanaTamaKayoSanaMaliKami",
        "hashtags": ["#SanaTamaKayoSanaMaliKami"],
        "timestamp": "2025-01-01T10:03:00",
    },
    {
        "user": "410399243094530",
        "text": "News update #Top5Balita #News5 #SaraDuterte #DOH",
        "hashtags": [
            "#Top5Balita",
            "#News5",
            "#SaraDuterte",
            "#DOH",
        ],
        "timestamp": "2025-01-01T10:04:00",
    },
]


print(f"Sending {len(posts)} hashtag-containing posts...")


for index, post in enumerate(posts, start=1):

    producer.send(
        TOPIC_NAME,
        value=post,
    )

    print(
        f"Sent {index}/{len(posts)} | "
        f"hashtags={post['hashtags']}"
    )

    time.sleep(1)


producer.flush()
producer.close()

print(f"Done. Sent {len(posts)} hashtag-containing posts.")

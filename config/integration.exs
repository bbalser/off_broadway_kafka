use Mix.Config

config :off_broadway_kafka,
  divo: [
    {DivoKafka, [create_topics: "topic1:2:1", kafka_image_version: "2.12-2.2.1"]}
  ],
  divo_wait: [dwell: 700, max_tries: 50]

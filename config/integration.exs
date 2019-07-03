use Mix.Config

config :off_broadway_kafka,
  divo: [
    {DivoKafka, [create_topics: "topic1:1:1"]}
  ],
  divo_wait: [dwell: 700, max_tries: 50]

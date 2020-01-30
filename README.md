# OffBroadwayKafka

This library integrates the [Broadway](https://hexdocs.pm/broadway) data processing
pipeline library with [Kafka](https://kafka.apache.org/).

It communicates with Kafka using the [Elsa](https://hex.pm/packages/elsa)
Elixir library, which itself uses the [Brod](https://hex.pm/packages/brod)
Erlang library.

It can dynamically create Broadway stages on a per-topic or per-partition basis
for a given Kafka topic.

## Installation

Add `off_broadway_kafka` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_kafka, "~> 1.0"}
  ]
end
```

Docs can be found on [HexDocs](https://hexdocs.pm/off_broadway_kafka)
or generated with [ExDoc](https://github.com/elixir-lang/ex_doc).

## Examples

### ClassicHandler

This example uses Broadway in the traditional way.

It calls `Elsa.Supervisor.start_link/1` with the specified `kafka_config` to
set up a Kafka consumer. It receives Kafka messages and adds them as events to
the `OffBroadway.Kafka.Producer` Broadway Producer, which are then handled by
the specified Broadway pipeline.

```elixir
defmodule ClassicBroadway do
  use Broadway

  def start_link(opts) do
    kafka_config = [
      connection: :client1,
      endpoints: [localhost: 9092],
      group_consumer: [
        group: "classic",
        topics: ["topic1"],
        config: [
          begin_offset: :earliest
        ]
      ]
    ]

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Kafka.Producer, kafka_config},
        stages: 1
      ],
      processors: [
        default: [
          stages: 1
        ]
      ],
      context: %{pid: Keyword.get(opts, :pid)}
    )
  end

  def handle_message(processor, message, context) do
    send(context.pid, {:message, message})
    message
  end
end
```

### ShowtimeHandler

This example uses the `OffBroadway.Kafka` macro.

It starts a Broadway pipeline for each topic and partition for increased
concurrency processing events.

```elixir
defmodule ShowtimeBroadway do
  use OffBroadway.Kafka

  def kafka_config(_opts) do
    [
      connection: :per_partition,
      endpoints: [localhost: 9092],
      group_consumer: [
        group: "per_partition",
        topics: ["topic1"],
        config: [
          prefetch_count: 5,
          prefetch_bytes: 0,
          begin_offset: :earliest
        ]
      ]
    ]
  end

  def broadway_config(opts, topic, partition) do
    [
      name: :"broadway_per_partition_#{topic}_#{partition}",
      processors: [
        default: [
          stages: 5
        ]
      ],
      context: %{
        pid: Keyword.get(opts, :pid)
      }
    ]
  end

  def handle_message(processor, message, context) do
    send(context.pid, {:message, message})
    message
  end
end
```

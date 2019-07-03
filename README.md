# OffBroadwayKafka

A Kafka integration for the Broadway library. OffBroadwayKafka utilizes the Elsa library
as its interface to Kafka (which is itself an interface to the Brod library in Erlang).
to allow for dynamically creating Broadway stages on a per-topic or per-partition basis
for a given Kafka topic.

## Installation

If the package can be installed by adding `off_broadway_kafka` to your list of 
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_kafka, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/off_broadway_kafka](https://hexdocs.pm/off_broadway_kafka).


defmodule OffBroadway.Kafka.PerPartitionTest do
  use ExUnit.Case
  use Divo

  test "it lives!!!" do
    {:ok, _pid} = PerPartition.start_link(pid: self())

    Elsa.produce([localhost: 9092], "topic1", [{"key1", "value1"}], partition: 0)
    Elsa.produce([localhost: 9092], "topic1", [{"key2", "value2"}], partition: 1)

    assert_receive {:message, %Broadway.Message{data: %{key: "key1", value: "value1"}}}, 5_000
    assert_receive {:message, %Broadway.Message{data: %{key: "key2", value: "value2"}}}, 5_000
  end
end

defmodule PerPartition do
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
        default: [concurrency: 5]
      ],
      context: %{
        pid: Keyword.get(opts, :pid)
      }
    ]
  end

  def handle_message(_processor, message, context) do
    IO.inspect(message, label: "message")
    send(context.pid, {:message, message})
    message
  end
end

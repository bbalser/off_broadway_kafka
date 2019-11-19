defmodule OffBroadway.Kafka.ClassicTest do
  use ExUnit.Case
  use Divo

  test "it lives, classicly!" do
    {:ok, broadway} = ClassicBroadway.start_link(pid: self())

    Elsa.produce([localhost: 9092], "topic1", [{"key1", "value1"}], partition: 0)
    Elsa.produce([localhost: 9092], "topic1", [{"key2", "value2"}], partition: 1)

    assert_receive {:message, %Broadway.Message{data: %{key: "key1", value: "value1"}}}, 5_000
    assert_receive {:message, %Broadway.Message{data: %{key: "key2", value: "value2"}}}, 5_000

    ref = Process.monitor(broadway)
    Process.unlink(broadway)
    Process.exit(broadway, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
  end
end

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

  def handle_message(_processor, message, context) do
    IO.inspect(message, label: "message")
    send(context.pid, {:message, message})
    message
  end
end

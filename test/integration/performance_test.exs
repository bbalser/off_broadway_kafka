defmodule OffBroadwayKafka.PerformanceTest do
  use ExUnit.Case
  use Divo
  require Logger
  @moduletag :performance

  @num_messages 1000
  @num_partitions 1

  setup do
    Elsa.create_topic([localhost: 9092], "offbroadwaykafka-perf", partitions: @num_partitions)

    messages =
      0..@num_messages
      |> Enum.map(fn index -> {"#{index}", create_message(index)} end)

    Elsa.produce([localhost: 9092], "offbroadwaykafka-perf", messages)
  end

  @tag timeout: :infinity
  test "performance test" do
    {:ok, _, results} = Elsa.fetch([localhost: 9092], "offbroadwaykafka-perf", offset: 951)
    assert Enum.count(results) == 50
  end

  defp create_message(index) do
    "\{\"id\":\"id_#{index}\",\"name\":\"#{random_string(7)}\",\"age\":\"#{:crypto.rand_uniform(0,99)}\",\"description\":\"#{random_string(25)}\",\"address\":\"#{random_string(15)}\"}"
  end

  defp random_string(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64()
    |> binary_part(0, length)
  end
end

defmodule ClassicBroadway do
  use Broadway

  def start_link(opts) do
    kafka_config = [
      name: :client1,
      brokers: [localhost: 9092],
      group: "classic",
      topics: ["topic1"],
      config: [
        begin_offset: :earliest
      ]
    ]

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {OffBroadwayKafka.Producer, kafka_config},
          stages: 1
        ]
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
    IO.inspect(message, label: "message")
    send(context.pid, {:message, message})
    message
  end
end

defmodule OffBroadway.Kafka.PerformanceTest do
  use ExUnit.Case
  use Divo
  require Logger
  @moduletag :performance

  @num_messages 10_000
  @num_partitions 10
  @input_topic "offbroadwaykafka-perf"
  @output_topic "offbroadwaykafka-output"

  setup do
    Logger.configure(level: :warn)
    :ok = Elsa.create_topic([localhost: 9092], @input_topic, partitions: @num_partitions)
    :ok = Elsa.create_topic([localhost: 9092], @output_topic)

    Patiently.wait_for!(fn ->
      true == Elsa.topic?([localhost: 9092], @input_topic)
    end)

    {:ok, _elsa} =
      Elsa.Supervisor.start_link(
        connection: :elsa_writer,
        endpoints: [localhost: 9092],
        producer: [
          [topic: @input_topic],
          [topic: @output_topic]
        ]
      )

    :ok
  end

  @tag timeout: :infinity
  test "performance test" do
    Benchee.run(
      %{
        "classic" => fn %{stages: stages} -> classic_broadway(stages) end,
        "per_partition" => fn %{stages: stages} -> per_partition_broadway(stages) end
      },
      inputs: %{
        "1 stage" => %{stages: 1},
        "10 stages" => %{stages: 10},
        "100 stages" => %{stages: 100}
      },
      time: 10,
      memory_time: 2,
      after_each: fn _result -> Process.sleep(1_000) end
    )
  end

  defp per_partition_broadway(stages) do
    write_messages_to_input()

    starting_offset = latest_offset()

    {:ok, pid} =
      PerPartitionBroadway.start_link(processor_stages: stages, topics: [@input_topic], output_topic: @output_topic)

    Patiently.wait_for!(
      fn ->
        latest_offset = latest_offset()
        Logger.warn("starting_offset: #{starting_offset} - latest offset: #{latest_offset}")
        latest_offset - starting_offset >= @num_messages
      end,
      dwell: 200,
      max_tries: 10_000
    )

    ensure_exit(pid)
  end

  defp classic_broadway(stages) do
    write_messages_to_input()
    starting_offset = latest_offset()

    {:ok, pid} =
      ClassicPerfBroadway.start_link(processor_stages: stages, topics: [@input_topic], output_topic: @output_topic)

    Patiently.wait_for!(
      fn ->
        latest_offset = latest_offset()
        Logger.warn("starting_offset: #{starting_offset} - latest offset: #{latest_offset}")
        latest_offset - starting_offset >= @num_messages
      end,
      dwell: 200,
      max_tries: 10_000
    )

    ensure_exit(pid)
  end

  defp ensure_exit(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
  end

  defp write_messages_to_input() do
    messages =
      0..(@num_messages - 1)
      |> Enum.map(fn index -> {"#{index}", create_message(index)} end)

    :ok = Elsa.produce(:elsa_writer, @input_topic, messages, partitioner: :random)
  end

  defp latest_offset() do
    {:ok, latest_offset} = :brod.resolve_offset([localhost: 9092], @output_topic, 0, :latest)
    latest_offset
  end

  defp create_message(index) do
    "\{\"id\":\"id_#{index}\",\"name\":\"#{random_string(7)}\",\"age\":\"#{:crypto.rand_uniform(0, 99)}\",\"description\":\"#{
      random_string(25)
    }\",\"address\":\"#{random_string(15)}\"}"
  end

  defp random_string(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64()
    |> binary_part(0, length)
  end
end

defmodule ClassicPerfBroadway do
  use Broadway

  def start_link(opts) do
    kafka_config = [
      connection: :client1,
      endpoints: [localhost: 9092],
      group_consumer: [
        group: "group1",
        topics: Keyword.get(opts, :topics),
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
          stages: Keyword.get(opts, :processor_stages, 1)
        ]
      ],
      context: %{
        topic: Keyword.get(opts, :output_topic)
      }
    )
  end

  def handle_message(_processor, message, context) do
    Elsa.produce(:elsa_writer, context.topic, message.data.value, partition: 0)
    message
  end
end

defmodule PerPartitionBroadway do
  use OffBroadway.Kafka

  def kafka_config(opts) do
    [
      connection: :per_partition,
      endpoints: [localhost: 9092],
      group_consumer: [
        group: "group1",
        topics: Keyword.get(opts, :topics),
        config: [
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
          stages: Keyword.get(opts, :processor_stages, 1)
        ]
      ],
      context: %{
        topic: Keyword.get(opts, :output_topic)
      }
    ]
  end

  def handle_message(_processor, message, context) do
    Elsa.produce(:elsa_writer, context.topic, message.data.value, partition: 0)
    message
  end
end

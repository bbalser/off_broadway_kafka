defmodule OffBroadway.Kafka.AckingEventTest do
  use ExUnit.Case
  use Divo
  require Logger

  @endpoints [localhost: 9092]
  @topic "ack-order-test"
  @group "ack-order-group"

  setup do
    Elsa.create_topic(@endpoints, @topic, partitions: 3)
    :ok
  end

  test "should ack events in order" do
    {:ok, broadway} = AckingEventBroadway.start_link(pid: self())

    message_counts = %{
      0 => random(10_000..11_000),
      1 => random(5_000..6_000),
      2 => random(20_000..22_000)
    }

    Enum.each(message_counts, fn {partition, count} ->
      messages = Enum.map(1..count, fn idx -> {"key#{idx}", "value#{idx}"} end)
      Elsa.produce(@endpoints, @topic, messages, partition: partition)
    end)

    Enum.each(message_counts, fn {partition, count} ->
      assert_group_offset(partition, count)
    end)
  end

  defp random(range) do
    start..stop = range
    (:rand.uniform() * (stop - start) + start) |> round
  end

  defp assert_group_offset(partition, offset) do
    Patiently.wait_for!(
      fn ->
        group_offset = get_group_offset(partition)
        Logger.info("Current group offset for partition #{partition} is #{group_offset}")
        offset == group_offset
      end,
      dwell: 500,
      max_tries: 20
    )
  end

  defp get_group_offset(partition) do
    case :brod.fetch_committed_offsets(:dup_client, @group) do
      {:error, _} ->
        :undefined

      {:ok, []} ->
        :undefined

      {:ok, topics} ->
        topics
        |> Enum.find(fn %{topic: topic} -> topic == @topic end)
        |> Map.get(:partition_responses)
        |> Enum.find(fn %{partition: p} -> p == partition end)
        |> Map.get(:offset)
    end
  end
end

defmodule AckingEventBroadway do
  use Broadway

  @endpoints [localhost: 9092]
  @topic "ack-order-test"
  @group "ack-order-group"

  def start_link(opts) do
    kafka_config = [
      name: :dup_client,
      brokers: @endpoints,
      group: @group,
      topics: [@topic],
      config: [
        begin_offset: :earliest
      ]
    ]

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {OffBroadway.Kafka.Producer, kafka_config},
          stages: 1
        ]
      ],
      processors: [
        default: [
          stages: 1
        ]
      ],
      batchers: [
        default: [
          stages: 1,
          batch_size: 1_000,
          batch_timeout: 2_000
        ]
      ],
      context: %{pid: Keyword.get(opts, :pid)}
    )
  end

  def handle_message(_processor, message, context) do
    send(context.pid, {:message, message})
    message
  end

  def handle_batch(_batcher, messages, _batch_info, context) do
    send(context.pid, {:batch, messages})
    messages
  end
end

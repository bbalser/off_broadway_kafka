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

  @tag timeout: :infinity
  test "should ack events in order" do
    {:ok, message_store} = MessageStore.start_link([])
    {:ok, broadway} = AckingEventBroadway.start_link(pid: message_store, name: :first_broadway)

    message_counts = %{
      0 => random(10_000..11_000),
      1 => random(5_000..6_000),
      2 => random(20_000..22_000)
    }

    Enum.each(message_counts, fn {partition, count} ->
      messages = Enum.map(1..count, fn idx -> {"key#{idx}", "value#{idx}"} end)
      Elsa.produce(@endpoints, @topic, messages, partition: partition)
    end)

    Process.sleep(2_000)

    {:ok, broadway} = AckingEventBroadway.start_link(pid: message_store, name: :second_broadway)

    Process.sleep(30_000)

    Enum.each(message_counts, fn {partition, count} ->
      assert_messages_received(partition, count)
    end)

    Enum.each(message_counts, fn {partition, count} ->
      assert_group_offset(partition, count)
    end)
  end

  defp random(range) do
    start..stop = range
    (:rand.uniform() * (stop - start) + start) |> round
  end

  defp assert_messages_received(partition, count) do
    Patiently.wait_for!(
      fn ->
        messages = MessageStore.get_counts(partition)
        Logger.info("Currently received #{length(messages)} for partition #{partition}")
        length(messages) == count
      end,
      dwell: 500,
      max_tries: 20
    )
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
    case :brod.fetch_committed_offsets(:first_broadway, @group) |> IO.inspect(label: "commited offsets") do
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

defmodule MessageStore do
  use GenServer
  require Logger

  def get_counts(partition) do
    GenServer.call(__MODULE__, {:get_counts, partition})
  end

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    {:ok, %{}}
  end

  def handle_call({:get_counts, partition}, _from, state) do
    t = Enum.map(state, fn {partition, messages} -> {partition, length(messages)} end) |> Enum.into(%{})
    Logger.debug(fn -> "Current counts : #{inspect(t)}" end)
    messages = Map.get(state, partition, [])
    {:reply, messages, state}
  end

  def handle_info({:message, message}, state) do
    partition = message.data.partition
    {:noreply, Map.update(state, partition, [message], fn msgs -> msgs ++ [message] end)}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end

defmodule AckingEventBroadway do
  use Broadway

  @endpoints [localhost: 9092]
  @topic "ack-order-test"
  @group "ack-order-group"

  def start_link(opts) do
    kafka_config = [
      name: Keyword.fetch!(opts, :name),
      brokers: @endpoints,
      group: @group,
      topics: [@topic],
      config: [
        begin_offset: :earliest,
        prefetch_bytes: 0,
        prefetch_count: 100,
        session_timeout_seconds: 120
      ]
    ]

    Broadway.start_link(__MODULE__,
      name: :"#{Keyword.fetch!(opts, :name)}_broadway",
      producers: [
        default: [
          module: {OffBroadway.Kafka.Producer, kafka_config},
          stages: 1
        ]
      ],
      processors: [
        default: [
          stages: 8
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
    Process.sleep(1)
    send(context.pid, {:message, message})
    message
  end

  def handle_batch(_batcher, messages, _batch_info, context) do
    send(context.pid, {:batch, messages})
    messages
  end
end

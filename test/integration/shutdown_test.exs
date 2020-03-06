defmodule OffBroadway.KafkaShutdownTest do
  use ExUnit.Case
  use Divo
  require Logger

  @endpoints [localhost: 9092]
  @topic "shutdown-test"
  @group "shutdown-group"

  setup do
    {:ok, pid} = Agent.start_link(fn -> 0 end, name: :shutdown_outbound_tracker)
    Elsa.create_topic(@endpoints, @topic)

    on_exit(fn ->
      kill_and_wait(pid)
    end)

    :ok
  end

  test "shutdown properly drains messages" do
    {:ok, broadway} = ShutdownBroadway.start_link(pid: self())

    messages = Enum.map(1..100_000, fn idx -> {"key#{idx}", "value#{idx}"} end)
    Elsa.produce(@endpoints, @topic, messages, partition: 0)

    Patiently.wait_for(
      fn ->
        Agent.get(:shutdown_outbound_tracker, fn s -> s end) > 1_000
      end,
      dwell: 100,
      max_tries: 100
    )

    kill_and_wait(broadway)

    count_sent = Agent.get(:shutdown_outbound_tracker, fn s -> s end)
    Logger.error("Number sent to outbound tracker : #{count_sent}")

    Fake.Consumer.Group.start_link(group: @group, topic: @topic, pid: self())

    assert_receive {:group_offset, group_offset}, 5_000

    assert count_sent == group_offset
  end

  defp kill_and_wait(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}, 10_000
  end
end

defmodule ShutdownBroadway do
  use Broadway

  @endpoints [localhost: 9092]
  @topic "shutdown-test"
  @group "shutdown-group"

  def start_link(opts) do
    kafka_config = [
      connection: :shutdown_client,
      endpoints: @endpoints,
      group_consumer: [
        group: @group,
        topics: [@topic],
        config: [
          begin_offset: :earliest
        ]
      ]
    ]

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Kafka.Producer, kafka_config},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: 1
        ]
      ],
      batchers: [
        default: [
          concurrency: 1,
          batch_size: 1_000,
          batch_timeout: 2_000
        ]
      ],
      context: %{pid: Keyword.get(opts, :pid)}
    )
  end

  def handle_message(_processor, message, _context) do
    message
  end

  def handle_batch(_batcher, messages, _batch_info, _context) do
    count = length(messages)
    Agent.update(:shutdown_outbound_tracker, fn s -> s + count end)
    messages
  end
end

defmodule Fake.Consumer.Group do
  use GenServer

  @client :fake_consumer_group

  def get_committed_offsets(_pid, _topic), do: {:ok, []}

  def assignments_revoked(_pid), do: :ok

  def assignments_received(pid, group, generation_id, assignments) do
    GenServer.cast(pid, {:assignment, group, generation_id, assignments})
  end

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(arg) do
    {:ok, Enum.into(arg, %{}), {:continue, :start}}
  end

  def handle_continue(:start, state) do
    {:ok, _client_pid} = :brod.start_link_client([localhost: 9092], @client)

    {:ok, _group_coordinator_pid} =
      :brod_group_coordinator.start_link(@client, state.group, [state.topic], [], __MODULE__, self())

    {:noreply, state}
  end

  def handle_cast({:assignment, _group, _generation_id, assignments}, state) do
    [{:brod_received_assignment, _, _, offset}] = assignments
    send(state.pid, {:group_offset, offset})
    {:noreply, state}
  end
end

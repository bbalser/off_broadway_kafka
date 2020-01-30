defmodule OffBroadway.Kafka.Producer do
  @moduledoc """
  Defines a Broadway Producer module which receives messages from Kafka
  to initiate the Broadway pipeline.

  Sends messages to the `handle_info/2` and `handle_demand/2` functions based
  on requests and tracks acknowledgements in state.
  """
  use GenStage
  use Retry
  require Logger

  @doc """
  Passes Kafka messages to the `handle_info/2` function.

  Called by Elsa message handler.
  """
  @spec handle_messages(pid(), term()) :: :ok
  def handle_messages(pid, messages) do
    send(pid, {:process_messages, messages})
  end

  @doc """
  Passes Kafka partition assignment changes to the `handle_info/2` function.

  Called by Elsa `assignments_revoked_handler`.
  """
  @spec assignments_revoked(pid()) :: :ok
  def assignments_revoked(pid) do
    send(pid, {:assignments_revoked, self()})

    receive do
      {:assignments_revoked, :complete} -> :ok
    end
  end

  @doc """
  Starts an `OffBroadway.Kafka` producer process linked to the current
  process.
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @doc """
  Names the producer process and initializes state for the producer GenServer.

  If `args` contains a value for `:endpoints`, creates a handler config and
  passes it to the Elsa library to start a consumer group supervisor and stores
  the returned `pid` in the Broadway Producer state for reference.
  """
  @impl GenStage
  def init(args) do
    connection = Keyword.fetch!(args, :connection)

    state = %{
      demand: 0,
      events: [],
      connection: connection,
      acknowledgers: %{},
      elsa_sup_pid: maybe_start_elsa(args)
    }

    {:producer, state}
  end

  @doc """
  Handles message events based on demand.

  Updates the demand based on the existing demand in state and sends the
  requested number of message events.
  """
  @impl GenStage
  def handle_demand(demand, state) do
    total_demand = demand + state.demand
    send_events(state, total_demand, state.events)
  end

  @doc """
  Handles incoming Kafka message events.

  Updates the total message events based on existing messages and incoming
  messages and sends the requested events.
  """
  @impl GenStage
  def handle_info({:process_messages, messages}, state) do
    total_events = state.events ++ messages
    send_events(state, state.demand, total_events)
  end

  @doc """
  Handles assignments revoked by the Kafka broker.

  Waits until acknowledgers report as empty and cleans out any events in queue.
  """
  @impl GenStage
  def handle_info({:assignments_revoked, caller}, state) do
    acknowledgers = Map.values(state.acknowledgers)

    wait constant_backoff(100) do
      Enum.all?(acknowledgers, &OffBroadway.Kafka.Acknowledger.is_empty?/1)
    after
      _ -> true
    else
      _ -> false
    end

    send(caller, {:assignments_revoked, :complete})
    {:noreply, [], %{state | events: []}}
  end

  @doc """
  Handles termination of the Elsa consumer group supervisor process if one exists.
  """
  @impl GenStage
  def terminate(reason, %{elsa_sup_pid: elsa}) when is_pid(elsa) do
    Logger.error("#{__MODULE__}: Terminated elsa")
    Supervisor.stop(elsa)
    reason
  end

  def terminate(reason, _state) do
    reason
  end

  defp maybe_start_elsa(opts) do
    producer_pid = self()

    if Keyword.has_key?(opts, :endpoints) do
      endpoints = Keyword.fetch!(opts, :endpoints)
      group_consumer = Keyword.fetch!(opts, :group_consumer)

      if Keyword.get(opts, :create_topics, false) == true do
        topics = Keyword.fetch!(group_consumer, :topics)
        ensure_topic(endpoints, topics)
      end

      new_group_consumer =
        group_consumer
        |> Keyword.put(:handler, OffBroadway.Kafka.ClassicHandler)
        |> Keyword.put(:handler_init_args, %{producer: self()})
        |> Keyword.put(:assignments_revoked_handler, fn ->
          OffBroadway.Kafka.Producer.assignments_revoked(producer_pid)
        end)
        |> Keyword.put(:direct_ack, true)

      config = Keyword.put(opts, :group_consumer, new_group_consumer)

      Logger.info("Starting Elsa from Producer - #{inspect(config)}")
      {:ok, pid} = Elsa.Supervisor.start_link(config)
      pid
    end
  end

  defp ensure_topic(endpoints, topics) do
    Enum.each(topics, fn topic ->
      unless Elsa.topic?(endpoints, topic) do
        Elsa.create_topic(endpoints, topic)
      end
    end)
  end

  defp send_events(state, total_demand, total_events) do
    events_to_send = Enum.take(total_events, total_demand)
    num_events_to_send = length(events_to_send)

    new_state =
      %{
        state
        | demand: total_demand - num_events_to_send,
          events: Enum.drop(total_events, num_events_to_send)
      }
      |> ensure_acknowledgers(events_to_send)

    broadway_messages = wrap_events(new_state, events_to_send)
    add_offsets_to_acknowledger(broadway_messages)

    {:noreply, broadway_messages, new_state}
  end

  defp ensure_acknowledgers(state, events) do
    events
    |> Enum.reduce(MapSet.new(), fn event, set -> MapSet.put(set, OffBroadway.Kafka.Acknowledger.ack_ref(event)) end)
    |> Enum.reduce(state, fn ack_ref, acc ->
      case Map.has_key?(acc.acknowledgers, ack_ref) do
        true ->
          acc

        false ->
          {:ok, pid} = OffBroadway.Kafka.Acknowledger.start_link(connection: acc.connection)
          %{acc | acknowledgers: Map.put(acc.acknowledgers, ack_ref, pid)}
      end
    end)
  end

  defp add_offsets_to_acknowledger([]), do: nil

  defp add_offsets_to_acknowledger(broadway_messages) do
    broadway_messages
    |> Enum.map(fn %Broadway.Message{acknowledger: {_, ack_ref, %{offset: offset}}} -> {ack_ref, offset} end)
    |> Enum.group_by(fn {ack_ref, _} -> ack_ref end, fn {_, offset} -> offset end)
    |> Enum.map(fn {ack_ref, offsets} -> {ack_ref, Enum.min_max(offsets)} end)
    |> Enum.each(fn {ack_ref, {min, max}} ->
      OffBroadway.Kafka.Acknowledger.add_offsets(ack_ref.pid, min..max)
    end)
  end

  defp wrap_events(state, messages) do
    messages
    |> Enum.map(fn message ->
      ack_ref = OffBroadway.Kafka.Acknowledger.ack_ref(message)

      acknowledger = Map.get(state.acknowledgers, ack_ref)

      %Broadway.Message{
        data: message,
        acknowledger: {OffBroadway.Kafka.Acknowledger, Map.put(ack_ref, :pid, acknowledger), %{offset: message.offset}}
      }
    end)
  end
end

defmodule OffBroadway.Kafka.Producer do
  @moduledoc """
  Implements the logic to handle incoming messages through
  the broadway pipeline. Sends messages to the `handle_info/2`
  and `handle_demand/2` functions based on requests and tracks
  acknowledgements in state.
  """
  use GenStage
  require Logger

  @doc """
  Convenience function for sending messages to be
  produced via the `handle_info/2` function.
  """
  @spec handle_messages(pid(), term()) :: :ok
  def handle_messages(pid, messages) do
    send(pid, {:process_messages, messages})
  end

  @doc """
  Starts an OffBroadway.Kafka producer process linked to the current
  process.
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @doc """
  Names the producer process and initializes state
  for the producer GenServer.

    * if args contain a value for :brokers or :endpoints,
    creates a handler config and passes it to the Elsa library
    to start a consumer group supervisor and store the returned
    pid in the Broadway Producer state for reference.
  """
  def init(args) do
    name = Keyword.fetch!(args, :name)

    state = %{
      demand: 0,
      events: [],
      name: name,
      acknowledgers: %{},
      elsa_sup_pid: maybe_start_elsa(args)
    }

    {:producer, state}
  end

  @doc """
  Handle message events based on demand. Updates the
  demand based on the existing demand in state and sends the
  requested number of message events.
  """
  @spec handle_demand(non_neg_integer(), map()) :: :ok
  def handle_demand(demand, state) do
    total_demand = demand + state.demand
    send_events(state, total_demand, state.events)
  end

  @doc """
  Handle message events based on incoming messages. Updates the
  total message events based on existing messages and incoming
  messages and sends the requested events.
  """
  @spec handle_info({:process_messages, [term()]}, map()) :: :ok
  def handle_info({:process_messages, messages}, state) do
    total_events = state.events ++ messages
    send_events(state, state.demand, total_events)
  end

  @doc """
  Handles termination of the Elsa consumer group supervisor process if one exists.
  """
  @spec terminate(term(), map()) :: :ok
  def terminate(reason, %{name: name, elsa_sup_pid: elsa}) when is_pid(elsa) do
    Elsa.Group.Supervisor.stop(name)
    reason
  end

  def terminate(reason, _state) do
    reason
  end

  defp maybe_start_elsa(opts) do
    if Keyword.has_key?(opts, :brokers) || Keyword.has_key?(opts, :endpoints) do
      config =
        opts
        |> Keyword.put(:handler, OffBroadway.Kafka.ClassicHandler)
        |> Keyword.put(:handler_init_args, %{producer: self()})

      Logger.info("Starting Elsa from Producer - #{inspect(config)}")
      {:ok, pid} = Elsa.Group.Supervisor.start_link(config)
      pid
    end
  end

  defp send_events(state, total_demand, total_events) do
    events_to_send = Enum.take(total_events, total_demand)
    num_events_to_send = length(events_to_send)

    new_state = %{
      state
      | demand: total_demand - num_events_to_send,
        events: Enum.drop(total_events, num_events_to_send)
    }

    broadway_messages =
      state
      |> ensure_acknowledgers(events_to_send)
      |> wrap_events(events_to_send)

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
          {:ok, pid} = OffBroadway.Kafka.Acknowledger.start_link(name: acc.name)
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

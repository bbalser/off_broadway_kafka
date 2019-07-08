defmodule OffBroadwayKafka.Producer do
  use GenStage
  require Logger

  def handle_messages(pid, messages) do
    send(pid, {:process_messages, messages})
  end

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

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

  def handle_demand(demand, state) do
    total_demand = demand + state.demand
    send_events(state, total_demand, state.events)
  end

  def handle_info({:process_messages, messages}, state) do
    total_events = state.events ++ messages
    send_events(state, state.demand, total_events)
  end

  def terminate(reason, %{name: name, elsa_sup_pid: elsa}) when is_pid(elsa) do
    Elsa.Group.Supervisor.stop(name)
    reason
  end

  def terminate(reason, state) do
    reason
  end

  defp maybe_start_elsa(opts) do
    if Keyword.has_key?(opts, :brokers) || Keyword.has_key?(opts, :endpoints) do
      config =
        opts
        |> Keyword.put(:handler, OffBroadwayKafka.ClassicHandler)
        |> Keyword.put(:handler_init_args, %{producer: self()})

      Logger.error("Starting Elsa from Producer - #{inspect(config)}")
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
    |> Enum.reduce(MapSet.new(), fn event, set -> MapSet.put(set, OffBroadwayKafka.Acknowledger.ack_ref(event)) end)
    |> Enum.reduce(state, fn ack_ref, acc ->
      case Map.has_key?(acc.acknowledgers, ack_ref) do
        true ->
          acc

        false ->
          {:ok, pid} = OffBroadwayKafka.Acknowledger.start_link(name: acc.name)
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
      OffBroadwayKafka.Acknowledger.add_offsets(ack_ref.pid, min..max)
    end)
  end

  defp wrap_events(state, messages) do
    messages
    |> Enum.map(fn message ->
      ack_ref = OffBroadwayKafka.Acknowledger.ack_ref(message)

      acknowledger = Map.get(state.acknowledgers, ack_ref)

      %Broadway.Message{
        data: message,
        acknowledger: {OffBroadwayKafka.Acknowledger, Map.put(ack_ref, :pid, acknowledger), %{offset: message.offset}}
      }
    end)
  end
end

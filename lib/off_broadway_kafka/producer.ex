defmodule OffBroadwayKafka.Producer do
  use GenStage

  def producer_name(name, topic, partition) do
    :"off_broadway_producer_#{name}_#{topic}_#{partition}"
  end

  def handle_messages(pid, messages) do
    send(pid, {:process_messages, messages})
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenStage.start_link(__MODULE__, opts, name: producer_name(name, topic, partition))
  end

  def init(args) do
    {:ok, acknowledger} = OffBroadwayKafka.Acknowledger.start_link(args)

    state = %{
      demand: 0,
      events: [],
      acknowledger: acknowledger
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

  defp send_events(state, total_demand, total_events) do
    events_to_send = Enum.take(total_events, total_demand)
    num_events_to_send = length(events_to_send)

    new_state = %{
      state
      | demand: total_demand - num_events_to_send,
        events: Enum.drop(total_events, num_events_to_send)
    }

    add_offsets_to_acknowledger(state, events_to_send)

    {:noreply, wrap_events(state, events_to_send), new_state}
  end

  defp add_offsets_to_acknowledger(_state, []), do: nil

  defp add_offsets_to_acknowledger(state, events) do
    {min_offset, max_offset} =
      events
      |> Enum.map(fn event -> event.offset end)
      |> Enum.min_max()

    OffBroadwayKafka.Acknowledger.add_offsets(state.acknowledger, min_offset..max_offset)
  end

  defp wrap_events(state, messages) do
    messages
    |> Enum.map(fn message ->
      ack_ref = %{
        pid: state.acknowledger,
        topic: message.topic,
        partition: message.partition,
        generation_id: message.generation_id
      }

      %Broadway.Message{
        data: message,
        acknowledger: {OffBroadwayKafka.Acknowledger, ack_ref, %{offset: message.offset}}
      }
    end)
  end
end

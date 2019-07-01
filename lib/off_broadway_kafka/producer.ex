defmodule OffBroadwayKafka.Producer do
  use GenStage

  def handle_messages(pid, messages) do
    GenStage.cast(pid, {:process_messages, messages})
  end

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(args) do
    state = %{
      demand: 0,
      events: []
    }

    {:producer, args}
  end

  def handle_demand(demand, state) do
    total_demand = demand + state.demand
    events_to_send = Enum.take(state.events, total_demand)
    num_events_to_send = length(events_to_send)
    remaining_events = Enum.drop(state.events, num_events_to_send)

    {:noreply, events_to_send, %{state | demand: total_demand - num_events_to_send, events: remaining_events}}
  end

  def handle_cast({:process_messages, messages}, state) do
    total_events = state.events ++ messages
    events_to_send = Enum.take(total_events, state.demand)
    num_events_to_send = length(events_to_send)
    remaining_events = Enum.drop(total_events, num_events_to_send)

    {:noreply, events_to_send, %{state | demand: state.demand - num_events_to_send, events: remaining_events}}
  end
end

defmodule OffBroadwayKafka.MessageHandler do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    # Start Broadway
    {:ok, args}
  end

  def handle_messages(messages, state) do
    OffBroadwayKafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

defmodule OffBroadwayKafka.Acknowledger do
  @behaviour

  def ack(ack_ref, successful, failed) do
    :ok
  end
end

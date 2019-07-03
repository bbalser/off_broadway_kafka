defmodule OffBroadwayKafka.ProducerTest do
  use ExUnit.Case
  use Placebo

  describe "handle_info/2" do
    test "it adds incoming messages to its state" do
      events = create_messages(0..5)

      {:noreply, sent_events, new_state} =
        OffBroadwayKafka.Producer.handle_info({:process_messages, events}, state(0, []))

      assert [] == sent_events
      assert events == new_state.events
    end

    test "it returns events equals to demand" do
      events = create_messages(6..10)

      state = state(10, create_messages(1..5))

      {:noreply, sent_events, new_state} = OffBroadwayKafka.Producer.handle_info({:process_messages, events}, state)

      assert broadway_messages(1..10) == sent_events
      assert match?(%{demand: 0, events: []}, new_state)
    end

    test "sends incoming messages to the acknowledger" do
      allow OffBroadwayKafka.Acknowledger.add_offsets(any(), any()), return: :ok, meck_options: [:passthrough]

      events = create_messages(1..10)
      state = state(10, events)

      {:noreply, _sent_events, _new_state} = OffBroadwayKafka.Producer.handle_info({:process_messages, events}, state)

      assert_called OffBroadwayKafka.Acknowledger.add_offsets(:acknowledger_pid, 1..10)
    end
  end

  describe "handle_demand/2" do
    test "it adds demand to pending demand when no events available" do
      incoming_state = %{
        demand: 2,
        events: []
      }

      assert OffBroadwayKafka.Producer.handle_demand(5, incoming_state) == {:noreply, [], %{demand: 7, events: []}}
    end

    test "it returns events when pending events are available" do
      events = create_messages(1..10, topic: "test-topic")

      {:noreply, sent_events, new_state} = OffBroadwayKafka.Producer.handle_demand(5, state(2, events))

      expected_events = Enum.drop(events, 7)

      assert Enum.take(broadway_messages(events), 7) == sent_events
      assert match?(%{demand: 0, events: ^expected_events}, new_state)
    end

    test "it has pending demand if not enough events availabe" do
      events = create_messages(1..5)

      {:noreply, sent_events, new_state} = OffBroadwayKafka.Producer.handle_demand(8, state(0, events))

      assert broadway_messages(events) == sent_events
      assert match?(%{demand: 3, events: []}, new_state)
    end

    test "it resets demand and empties events when all messages are requested" do
      events = create_messages(1..10)

      {:noreply, sent_events, new_state} = OffBroadwayKafka.Producer.handle_demand(5, state(5, events))

      assert broadway_messages(events) == sent_events
      assert match?(%{demand: 0, events: []}, new_state)
    end
  end

  defp state(demand, events) do
    acknowledgers = Enum.reduce(events, %{}, fn event, acc ->
      ack_ref = OffBroadwayKafka.Acknowledger.ack_ref(event)
      Map.put(acc, ack_ref, :acknowledger_pid)
    end)
    %{
      demand: demand,
      events: events,
      acknowledgers: acknowledgers
    }
  end

  defp broadway_messages(messages) when is_list(messages) do
    messages
    |> Enum.map(fn %{offset: offset} = message ->
      %Broadway.Message{
        data: message,
        acknowledger:
          {OffBroadwayKafka.Acknowledger,
           %{
             pid: :acknowledger_pid,
             topic: message.topic,
             partition: message.partition,
             generation_id: message.generation_id
           }, %{offset: offset}}
      }
    end)
  end

  defp broadway_messages(range, opts \\ []) do
    create_messages(range, opts)
    |> broadway_messages()
  end

  defp create_messages(range, opts \\ []) do
    range
    |> Enum.map(fn i ->
      opts
      |> Keyword.put(:offset, i)
      |> Keyword.put(:key, "key - #{i}")
      |> Keyword.put(:value, "value - #{i}")
      |> create_message()
    end)
  end

  defp create_message(opts) do
    %{
      topic: Keyword.get(opts, :topic, "topic-a"),
      partition: Keyword.get(opts, :partition, 0),
      offset: Keyword.get(opts, :offset, 0),
      key: Keyword.get(opts, :key, ""),
      value: Keyword.get(opts, :value, "value"),
      generation_id: Keyword.get(opts, :generation_id, 0)
    }
  end
end

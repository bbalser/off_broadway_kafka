defmodule OffBroadwayKafka.ProducerTest do
  use ExUnit.Case

  describe "handle_cast/2" do
    test "it adds incoming messages to its state" do
      events = create_messages(0..5)
      state = %{events: [], demand: 0}

      assert OffBroadwayKafka.Producer.handle_cast({:process_messages, events}, state) ==
               {:noreply, [], %{state | events: events}}
    end

    test "it returns events equals to demand" do
      events = create_messages(6..10)

      state = %{
        events: create_messages(1..5),
        demand: 10
      }

      assert OffBroadwayKafka.Producer.handle_cast({:process_messages, events}, state) ==
               {:noreply, create_messages(1..10), %{demand: 0, events: []}}
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

      incoming_state = %{
        demand: 2,
        events: events
      }

      assert OffBroadwayKafka.Producer.handle_demand(5, incoming_state) ==
               {:noreply, Enum.take(events, 7), %{demand: 0, events: Enum.drop(events, 7)}}
    end

    test "it has pending demand if not enough events availabe" do
      events = create_messages(1..5)

      incoming_state = %{
        demand: 0,
        events: events
      }

      assert OffBroadwayKafka.Producer.handle_demand(8, incoming_state) ==
               {:noreply, events, %{demand: 3, events: []}}
    end

    test "it resets demand and empties events when all messages are requested" do
      events = create_messages(1..10)

      incoming_state = %{
        demand: 5,
        events: events
      }

      assert OffBroadwayKafka.Producer.handle_demand(5, incoming_state) ==
               {:noreply, events, %{demand: 0, events: []}}
    end
  end

  defp create_messages(range, opts \\ []) do
    range
    |> Enum.map(fn i ->
      opts
      |> Keyword.put(:offset, i)
      |> Keyword.put(:key, "key - #{i}")
      |> Keyword.put(:value, "value - #{i}")
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

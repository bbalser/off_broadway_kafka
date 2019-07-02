defmodule OffBroadwayKafka.AcknowledgerTest do
  use ExUnit.Case
  use Placebo

  alias OffBroadwayKafka.Acknowledger

  @name :name
  @topic "topic-1"
  @partition 0
  @generation_id 7

  setup do
    allow Elsa.Group.Manager.ack(any(), any(), any(), any(), any()), return: :ok

    {:ok, pid} =
      Acknowledger.start_link(name: @name, topic: @topic, partition: @partition, generation_id: @generation_id)

    [pid: pid]
  end

  test "should ack offsets as acknowledged", %{pid: pid} do
    Acknowledger.add_offsets(pid, 1..100)

    Acknowledger.ack(%{pid: pid}, [broadway_message(1)], [])

    Patiently.wait_for!(
      fn ->
        called?(Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, 1))
      end,
      dwell: 200,
      max_tries: 10
    )
  end

  test "should not ack offset if all previous offsets have not been acked", %{pid: pid} do
    Acknowledger.add_offsets(pid, 1..100)

    Acknowledger.ack(%{pid: pid}, [broadway_message(3)], [])

    Process.sleep(1_000)
    refute_called Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, any())
  end

  test "should ack all messages up to the latest that have been processed", %{pid: pid} do
    Acknowledger.add_offsets(pid, 1..100)

    Acknowledger.ack(%{pid: pid}, broadway_messages(1..3), [])

    Patiently.wait_for!(
      fn ->
        called?(Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, any()), once())
        called?(Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, 3))
      end,
      dwell: 200,
      max_tries: 10
    )
  end

  test "failed messages are also acked", %{pid: pid} do
    Acknowledger.add_offsets(pid, 1..100)

    Acknowledger.ack(%{pid: pid}, broadway_messages(1..27), broadway_messages(28..41))

    Patiently.wait_for!(
      fn ->
        called?(Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, any()), once())
        called?(Elsa.Group.Manager.ack(@name, @topic, @partition, @generation_id, 41))
      end,
      dwell: 200,
      max_tries: 10
    )
  end

  defp broadway_messages(range) do
    Enum.map(range, &broadway_message/1)
  end

  defp broadway_message(offset) do
    %Broadway.Message{
      data: nil,
      acknowledger: {:module, :ack_ref, %{offset: offset}}
    }
  end
end

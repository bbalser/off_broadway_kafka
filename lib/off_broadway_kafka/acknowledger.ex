defmodule OffBroadwayKafka.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger
  use GenServer
  require Logger

  def ack_ref(%{topic: topic, partition: partition, generation_id: generation_id}) do
    %{topic: topic, partition: partition, generation_id: generation_id}
  end

  @impl Broadway.Acknowledger
  def ack(%{pid: pid} = ack_ref, successful, failed) do
    offsets =
      successful
      |> Enum.concat(failed)
      |> Enum.map(fn %{acknowledger: {_, _, %{offset: offset}}} -> offset end)

    GenServer.cast(pid, {:ack, ack_ref, offsets})
  end

  def add_offsets(pid, range) do
    GenServer.cast(pid, {:add_offsets, range})
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(args) do
    state = %{
      name: Keyword.fetch!(args, :name),
      table: :ets.new(nil, [:ordered_set, :protected])
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:add_offsets, offsets}, state) do
    offsets
    |> Enum.each(fn offset -> :ets.insert(state.table, {offset, false}) end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:ack, ack_ref, offsets}, state) do
    Enum.each(offsets, fn offset ->
      :ets.insert(state.table, {offset, true})
    end)

    case get_offset_to_ack(state.table) do
      nil ->
        nil

      offset ->
        Logger.debug(
          "Acking(#{inspect(self())}) [topic: #{ack_ref.topic}, partition: #{ack_ref.partition}, offset: #{offset}]"
        )

        Elsa.Group.Manager.ack(state.name, ack_ref.topic, ack_ref.partition, ack_ref.generation_id, offset)
    end

    {:noreply, state}
  end

  defp get_offset_to_ack(table, previous \\ nil) do
    key = :ets.first(table)

    case :ets.lookup(table, key) do
      [{^key, true}] ->
        :ets.delete(table, key)
        get_offset_to_ack(table, key)

      _ ->
        previous
    end
  end
end

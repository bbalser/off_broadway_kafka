defmodule OffBroadwayKafka.Acknowledger do
  @behaviour Broadway.Acknowledger
  use GenServer
  require Logger

  @impl Broadway.Acknowledger
  def ack(%{pid: pid}, successful, failed) do
    offsets =
      successful
      |> Enum.concat(failed)
      |> Enum.map(fn %{acknowledger: {_, _, %{offset: offset}}} -> offset end)

    GenServer.cast(pid, {:ack, offsets})
  end

  def add_offsets(pid, range) do
    GenServer.cast(pid, {:add_offsets, range})
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(args) do
    topic = Keyword.fetch!(args, :topic)
    partition = Keyword.fetch!(args, :partition)

    state = %{
      name: Keyword.fetch!(args, :name),
      topic: topic,
      partition: partition,
      generation_id: Keyword.fetch!(args, :generation_id),
      table: :ets.new(:"#{__MODULE__}_#{topic}_#{partition}", [:ordered_set, :protected])
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
  def handle_cast({:ack, offsets}, state) do
    Enum.each(offsets, fn offset ->
      :ets.insert(state.table, {offset, true})
    end)

    case get_offset_to_ack(state.table) do
      nil ->
        nil

      offset ->
        Logger.debug("Acking [topic: #{state.topic}, partition: #{state.partition}, offset: #{offset}]")
        Elsa.Group.Manager.ack(state.name, state.topic, state.partition, state.generation_id, offset)
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

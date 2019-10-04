defmodule OffBroadway.Kafka.Acknowledger do
  @moduledoc """
  Implements the Broadway acknowledger behaviour, handling acking of processed
  messages back to Kafka once they have been successfully processed. Message
  ack references are stored in ETS as an ordered set and acknowledgements are
  performed in the order received rather than the order processed to ensure that
  a failure of the Broadway pipeline does not allow messages received later but
  processed faster to erroneously mark lost messages as acknowledged when they
  should instead be reprocessed on recovery of the pipeline.
  """
  @behaviour Broadway.Acknowledger
  use GenServer
  require Logger

  @type ack_ref() :: %{topic: String.t(), partition: non_neg_integer(), generation_id: non_neg_integer() | nil}

  @doc """
  Constructs an ack_ref record for storing the status of message acknowledgement
  in ETS.
  """
  @spec ack_ref(Elsa.Message.t()) :: ack_ref()
  def ack_ref(%{topic: topic, partition: partition, generation_id: generation_id}) do
    %{topic: topic, partition: partition, generation_id: generation_id}
  end

  @doc """
  Acknowledges processed messages to Kafka. Due to Kafka's requirement to maintain
  message order for proper offset management, concatenates successful and failed
  messages together and stores the total offset to return for acknowledgement.
  """
  @spec ack(map(), [Broadway.Message.t()], [Broadway.Message.t()]) :: :ok
  @impl Broadway.Acknowledger
  def ack(%{pid: pid} = ack_ref, successful, failed) do
    offsets =
      successful
      |> Enum.concat(failed)
      |> Enum.map(fn %{acknowledger: {_, _, %{offset: offset}}} -> offset end)

    GenServer.call(pid, {:ack, ack_ref, offsets})
  end

  @doc """
  Adds a set of messages, represented by a contiguous
  range, to ETS for tracking acknowledgement in the proper order.
  """
  @spec add_offsets(pid(), Range.t()) :: :ok
  def add_offsets(pid, range) do
    GenServer.cast(pid, {:add_offsets, range})
  end

  def is_empty?(pid) do
    GenServer.call(pid, :is_empty?)
  end

  @doc """
  Creates an acknowledger GenServer process and links it to
  the current process.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(args) do
    state = %{
      connection: Keyword.fetch!(args, :connection),
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
  def handle_call({:ack, ack_ref, offsets}, _from, state) do
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

        Elsa.Group.Manager.ack(state.connection, ack_ref.topic, ack_ref.partition, ack_ref.generation_id, offset)
    end

    {:reply, :ok, state}
  end

  def handle_call(:is_empty?, _from, state) do
    result =
      case :ets.first(state.table) do
        :"$end_of_table" -> true
        _ -> false
      end

    {:reply, result, state}
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

defmodule OffBroadway.Kafka.ShowtimeHandler do
  @moduledoc """
  Defines an `Elsa.Consumer.MessageHandler` for integrating with Broadway
  which supports creating a pipeline per topic partition for increased
  concurrency.

  Configuration for Kafka and Broadway are handled by the
  `c:OffBroadway.Kafka.kafka_config/1` and
  `c:OffBroadway.Kafka.broadway_config/3` callbacks.

  Set up by the `OffBroadway.Kafka` macro.
  """
  use Elsa.Consumer.MessageHandler

  @doc """
  Initializes Broadway pipeline for `Elsa.Consumer.MessageHandler`.

  Assumes a single producer stage using the `OffBroadway.Kafka.Producer` module.
  Other Broadway configuration comes from the `c:OffBroadway.Kafka.broadway_config/3`callback.

  Retains a reference to the producer process in the state.
  """
  @impl Elsa.Consumer.MessageHandler
  def init(args) do
    broadway_module = Keyword.fetch!(args, :broadway_module)
    opts = Keyword.get(args, :opts, [])

    producer = [
      module: {OffBroadway.Kafka.Producer, [connection: connection()]},
      concurrency: 1
    ]

    broadway_config =
      apply(broadway_module, :broadway_config, [opts, topic(), partition()])
      |> Keyword.put(:producer, producer)

    {:ok, broadway_pid} = Broadway.start_link(broadway_module, broadway_config)

    state = %{
      producer: Broadway.Server.producer_names(broadway_pid) |> List.first()
    }

    {:ok, state}
  end

  @doc """
  Handles Kafka messages.

  Delegates message processing to `OffBroadway.Kafka.Producer.handle_messages/2`.
  """
  @impl true
  def handle_messages(messages, state) do
    OffBroadway.Kafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

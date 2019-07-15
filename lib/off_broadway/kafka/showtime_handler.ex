defmodule OffBroadway.Kafka.ShowtimeHandler do
  @moduledoc """
  Implements message handling using an opinionated
  interpretation of Broadway for interaction with Kafka.
  Intended for use in conjunction with the `__using__` macro
  provided by `OffBroadway.Kafka`.
  Assumes a single producer stage, preconfigured to use
  the `OffBroadway.Kafka.Producer` module and takes configuration
  for additional Broadway elements via the Elsa configurations
  passed through via the `kafka_config/1` function and those passed
  to Broadway via the `broadway_config/1` functions defined in
  the top-level module's behaviour callbacks.
  """
  use Elsa.Consumer.MessageHandler

  @doc """
  Assumes a single producer stage using the `OffBroadway.Kafka.Producer`
  module and collects other Broadway configuration passed in from the
  calling application's implementation of the `broadway_config/1` behaviour.
  Retains a reference to the producer process in the state via Broadway's
  built-in `Broadway.Server.get_random_producer/1` function to allow passing
  reference to the producer process to Elsa.
  """
  @spec init(keyword()) :: :ok
  def init(args) do
    broadway_module = Keyword.fetch!(args, :broadway_module)
    opts = Keyword.get(args, :opts, [])

    producers = [
      default: [
        module: {OffBroadway.Kafka.Producer, [name: name()]},
        stages: 1
      ]
    ]

    broadway_config =
      apply(broadway_module, :broadway_config, [opts, topic(), partition()])
      |> Keyword.put(:producers, producers)

    {:ok, broadway_pid} = Broadway.start_link(broadway_module, broadway_config)

    state = %{
      producer: Broadway.Server.get_random_producer(broadway_pid)
    }

    {:ok, state}
  end

  @doc """
  Delegates messages to the processed to the `Producer` message handler
  function.
  """
  @spec handle_messages([term()], map()) :: :ok
  def handle_messages(messages, state) do
    OffBroadway.Kafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

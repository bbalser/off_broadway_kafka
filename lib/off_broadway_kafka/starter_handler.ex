defmodule OffBroadwayKafka.StarterHandler do
  @moduledoc false
  use Elsa.Consumer.MessageHandler

  def init(args) do
    broadway_module = Keyword.fetch!(args, :broadway_module)
    opts = Keyword.get(args, :opts, [])

    producers = [
      default: [
        module: {OffBroadwayKafka.Producer, [name: name()]},
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

  def handle_messages(messages, state) do
    OffBroadwayKafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

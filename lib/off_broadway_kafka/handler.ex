defmodule OffBroadwayKafka.Handler do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    broadway_module = Keyword.fetch!(args, :broadway_module)

    producers = [
      default: [
        module:
          {OffBroadwayKafka.Producer,
           [name: name(), topic: topic(), partition: partition(), generation_id: generation_id()]},
        stages: 1
      ]
    ]

    broadway_config =
      apply(broadway_module, :broadway_config, [topic(), partition()])
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

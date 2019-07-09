defmodule OffBroadway.Kafka.ClassicHandler do
  @moduledoc false
  use Elsa.Consumer.MessageHandler

  def init(args) do
    {:ok, args}
  end

  def handle_messages(messages, state) do
    OffBroadway.Kafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

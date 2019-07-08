defmodule OffBroadwayKafka.ClassicHandler do
  @moduledoc false
  use Elsa.Consumer.MessageHandler

  def init(args) do
    {:ok, args}
  end

  def handle_messages(messages, state) do
    OffBroadwayKafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

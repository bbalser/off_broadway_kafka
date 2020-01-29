defmodule OffBroadway.Kafka.ClassicHandler do
  @moduledoc """
  Defines an `Elsa.Consumer.MessageHandler` for integrating with Broadway in the
  tradtional way, using Broadway directly.

  It supplies a simple pass-through init function and delegates message
  handling to the `OffBroadway.Kafka.Producer` module.

  Set up by `OffBroadway.Kafka.Producer`.
  """
  use Elsa.Consumer.MessageHandler

  @doc """
  Supplies a basic init function, delegating the rest to the
  default `Elsa.Consumer.MessageHandler` behavior.
  """
  @impl Elsa.Consumer.MessageHandler
  def init(args) do
    {:ok, args}
  end

  @doc """
  Handles Kafka messages.

  Delegates message processing to `OffBroadway.Kafka.Producer.handle_messages/2`.
  """
  @impl Elsa.Consumer.MessageHandler
  def handle_messages(messages, state) do
    OffBroadway.Kafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

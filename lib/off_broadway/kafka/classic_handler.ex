defmodule OffBroadway.Kafka.ClassicHandler do
  @moduledoc """
  Supplies a message handler for integrating with
  Broadway in a more traditional manner of including
  Broadway directly in the implementing application
  only supplying a handler module customized to the
  producing system. The "Classic" handler supplies a
  simple pass-through init function and delegates message
  handling to the OffBroadway.Kafka.Producer module.
  """
  use Elsa.Consumer.MessageHandler

  @doc """
  Supplies a basic init function, delegating the majority
  of setup to the included Elsa consumer handler using macro.
  """
  @spec init(keyword()) :: {:ok, keyword()}
  def init(args) do
    {:ok, args}
  end

  @doc """
  Delegates message processing to the `Producer` message handler
  function.
  """
  @spec handle_messages([term()], map()) :: :ok
  def handle_messages(messages, state) do
    OffBroadway.Kafka.Producer.handle_messages(state.producer, messages)
    {:no_ack, state}
  end
end

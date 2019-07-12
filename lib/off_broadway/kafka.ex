defmodule OffBroadway.Kafka do
  @moduledoc """
  Implements a macro for easily including the OffBroadway.Kafka
  library in your application and declaring the callbacks expected
  by the framework to configure it's Broadway and Kafka configurations.
  The OffBroadway.Kafka process creates an Elsa group supervisor as
  part of its setup to manage consumer group interaction with the cluster.
  """

  @callback broadway_config(keyword(), String.t(), non_neg_integer()) :: keyword()
  @callback kafka_config(term()) :: keyword()

  @doc """
  Macro to include the setup functionality for OffBroadway.Kafka and start
  an Elsa consumer group manager for each Broadway pipeline instantiated.
  """
  defmacro __using__(_opts) do
    quote do
      use Broadway
      @behaviour OffBroadway.Kafka

      def start_link(opts) do
        kafka_config =
          kafka_config(opts)
          |> Keyword.put(:handler, OffBroadway.Kafka.StarterHandler)
          |> Keyword.put(:handler_init_args, broadway_module: __MODULE__, opts: opts)

        Elsa.Group.Supervisor.start_link(kafka_config)
      end
    end
  end
end

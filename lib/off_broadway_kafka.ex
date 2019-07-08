defmodule OffBroadwayKafka do
  @callback broadway_config(keyword(), String.t(), integer()) :: keyword()
  @callback kafka_config(term()) :: keyword()

  defmacro __using__(_opts) do
    quote do
      use Broadway
      require Logger
      @behaviour OffBroadwayKafka

      def start_link(opts) do
        kafka_config =
          kafka_config(opts)
          |> Keyword.put(:handler, OffBroadwayKafka.StarterHandler)
          |> Keyword.put(:handler_init_args, broadway_module: __MODULE__, opts: opts)

        Elsa.Group.Supervisor.start_link(kafka_config)
      end
    end
  end
end

defmodule OffBroadway.Kafka do
  @moduledoc ~S"""
  Defines a macro to easily define a Kafka Broadway pipeline in your
  application, configuring Broadway and Kafka via callbacks.

  It starts a Broadway pipeline for each topic and partition for increased
  concurrency processing events, receiving partition assignments from the group
  coordinator and starting an Elsa group supervisor for each.

  It uses the following callbacks:

    1. `c:kafka_config/1` receives `start_link` options and
       returns the Kafka consumer configuration which is passed to
       `Elsa.Supervisor.start_link/1`.

    2. `c:broadway_config/3` receives a keyword list of configuration
       options, a topic and a partition. It returns the keyword
       list to configure the Broadway processors, batchers and contexts.
       Called by `OffBroadway.Kafka.ShowtimeHandler`.

  For example:

  ```elixir
  defmodule ShowtimeBroadway do
    use OffBroadway.Kafka

    def kafka_config(_opts) do
      [
        connection: :per_partition,
        endpoints: [localhost: 9092],
        group_consumer: [
          group: "per_partition",
          topics: ["topic1"],
          config: [
            prefetch_count: 5,
            prefetch_bytes: 0,
            begin_offset: :earliest
          ]
        ]
      ]
    end

    def broadway_config(opts, topic, partition) do
      [
        name: :"broadway_per_partition_#{topic}_#{partition}",
        processors: [
          default: [
            concurrency: 5
          ]
        ],
        context: %{
          pid: Keyword.get(opts, :pid)
        }
      ]
    end

    def handle_message(processor, message, context) do
      send(context.pid, {:message, message})
      message
    end
  end
  ```
  """

  @callback broadway_config(keyword(), String.t(), non_neg_integer()) :: keyword()
  @callback kafka_config(term()) :: keyword()

  @doc """
  Macro which starts pipeline for each Elsa consumer group manager instantiated.
  """
  defmacro __using__(_opts) do
    quote do
      use Broadway
      @behaviour OffBroadway.Kafka

      def start_link(opts) do
        kafka_config = kafka_config(opts)

        new_group_consumer =
          Keyword.fetch!(kafka_config, :group_consumer)
          |> Keyword.put(:handler, OffBroadway.Kafka.ShowtimeHandler)
          |> Keyword.put(:handler_init_args, broadway_module: __MODULE__, opts: opts)

        config = Keyword.put(kafka_config, :group_consumer, new_group_consumer)

        Elsa.Supervisor.start_link(config)
      end
    end
  end
end

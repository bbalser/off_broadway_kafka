defmodule OffBroadway.Kafka do
  @moduledoc ~S"""
  Implements a macro for easily including the OffBroadway.Kafka
  library in your application and declaring the callbacks expected
  by the framework to configure it's Broadway and Kafka configurations.
  The OffBroadway.Kafka process creates an Elsa group supervisor as
  part of its setup to manage consumer group interaction with the cluster.

  ## OffBroadway.Kafka behaviour

  To define the custom Broadway behaviour for OffBroadway.Kafka:

    1. Define a `broadway_config/3` function that receives a keyword list
       of general configuration values as well as a topic and a partition
       and constructs the necessary keyword list to configure the Broadway
       processers and any desired batchers and contexts.
    2. Define a `kafka_config/1` function that constructs the necessary
       configuration to pass to Elsa for defining which Kafka brokers to
       connect to, which topic and partition Broadway should consume from,
       a name and consumer group name, as well as consumer configs:
         * prefetch_count
         * prefetch_bytes
         * begin_offset

  ## Examples
  ### ClassicHandler
  This implementation uses Broadway directly and defines the handler to use
  the OffBroad.Kafka.Producer handler which communicates with the cluster via
  Elsa:

    defmodule ClassicBroadway do
      use Broadway

      def start_link(opts) do
        kafka_config = [
          connection: :client1,
          endpoints: [localhost: 9092],
          group_consumer: [
            group: "classic",
            topics: ["topic1"],
            config: [
              begin_offset: :earliest
            ]
          ]
        ]

        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            module: {OffBroadway.Kafka.Producer, kafka_config},
            stages: 1
          ],
          processors: [
            default: [
              stages: 1
            ]
          ],
          context: %{pid: Keyword.get(opts, :pid)}
        )
      end

      def handle_message(processor, message, context) do
        send(context.pid, {:message, message})
        message
      end
    end

  ### ShowtimeHandler
  This implementation uses the OffBroadway.Kafka behaviour and starts
  an Elsa group supervisor directly to have the handler create a per-topic,
  per-partition basis Broadway pipeline on receiving partition assignments
  from the group coordinator for nested concurrency of processing events:

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
              stages: 5
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

defmodule Neutron do
  @moduledoc """
  A C nif-based Apache Pulsar Client
  """

  alias Neutron.{PulsarAsyncProducer, PulsarClient, PulsarConsumer, PulsarNifs}

  @doc """
  Register and start Pulsar consumer under the Neutron supervisor tree

  Using a keyword list [callback_module: ModuleName] value.

  The callback needs to implement the PulsarConsumerCallback behaviour
  (@callback handle_message(String.t()) :: :ack | :ack_all | :nack)

  The callback handle_message function is applied to each message from Pulsar to this consumer.
  The callback handle_message function should return one of these three atoms:

  :ack
  :ack_all
  :nack

  :ack and :ack_all have relatively transparent counterparts in the Pulsar network API.
  However :nack does not, and should be used with great care (it is implemented client-side).

  A callback return value of :nack will cause the Pulsar client to accumulate a data structure of message ids.
  This periodically sends requests to Pulsar with the ids.
  There is no logic in the consumer to prevent this data structure from growing without bound.
  If the valid messages are not forthcoming, the Pulsar client /should/ run out of memory.
  As far as I know, this unfortunate flaw is inherent in the current design of Pulsar.

  Another argument in the keyword list is consumer type with the valid types of:
  :exclusive, :shared, :failover, or :key_shared
  They are passed with a [consumer_type: :shared] keyword list value.
  This defaults to :shared. Also :shared does not support :ack_all as expected due to the protocol.

  The last keyword list arguments are topic and subscription binaries ("strings"):
  i.e. [topic: "my-topic", subscription: "my-subscription"].
  The default topic is "my-topic" and the default subscription "my-subscription" fallbacks are included.
  """
  @spec start_consumer(Keyword.t()) :: DynamicSupervisor.on_start_child()
  def start_consumer(args \\ []) when is_list(args) do
    # in the C/C++ code the default is exclusive this defaults to shared subscription
    Neutron.Application.start_child(PulsarConsumer.child_spec(args))
  end

  @doc """
  Does a synchronous produce to given topic binary with given message binary and optional produce_opts.
  produce_opts: the keys are :deliver_after_ms and :deliver_at_ms both take an int as the value for :deliver_at_ms the int is a unix timestamp in milliseconds for :deliver_after_ms it's the delay to send the message after in milliseconds.
  Will bootstrap a pulsar producer for each call so async produce where you re-use the producer is more efficient.
  Uses the global pulsar client for connection information.
  """
  @spec sync_produce(String.t(), String.t(), map()) :: :ok | {:error, String.t()}
  def sync_produce(topic, message, produce_opts \\ %{})
      when is_binary(topic) and is_binary(message) and is_map(produce_opts) do
    {:ok, client_ref} = PulsarClient.get_client()
    PulsarNifs.sync_produce(client_ref, topic, message, produce_opts)
  end

  @doc """
  Creates an asynchronous producer genServer with associated topic and callback.
  Returns a pid to be managed and re-used by the callie.

  The callback function is applied to each message delivered from Pulsar to this producer.
  It needs to implement the PulsarAsyncProducerCallback behaviour.
  (@callback handle_delivery({:ok, String.t()} | {:error, String.t()}) :: any())

  The callback function's return value is ignored.
  It is either passed in {:ok, String.t()} | {:error, String.t()} depending on if the message was successfully produced or if there was an error.
  Where the String.t() is the serialized messageId which is comma seperated surrounded by parentheses.
  This can be re-serialized and passed back to pulsar, if needed:

  int64 ledgerId
  int64 entryId
  int32 partition
  int32 batchIndex
  """
  @spec create_async_producer(String.t(), atom(), GenServer.options()) :: GenServer.on_start()
  def create_async_producer(topic, callback_module, genserver_opts \\ [])
      when is_binary(topic) and is_atom(callback_module) and is_list(genserver_opts) do
    PulsarAsyncProducer.start_link(
      [topic: topic, callback_module: callback_module],
      genserver_opts
    )
  end

  @doc "Same args and options as `create_async_producer` but
    starts the async producer under the neutron supervisor tree with a dynamic supervisor."
  @spec create_managed_async_producer(String.t(), atom(), GenServer.options()) ::
          DynamicSupervisor.on_start_child()
  def create_managed_async_producer(topic, callback_module, genserver_opts \\ [])
      when is_binary(topic) and is_atom(callback_module) and is_list(genserver_opts) do
    child_spec =
      PulsarAsyncProducer.child_spec([
        [topic: topic, callback_module: callback_module],
        genserver_opts
      ])

    Neutron.Application.start_child(child_spec)
  end

  @doc """
  Does a asynchronous produce using the given producer pid generated from create_async_producer, given message binary and optional produce_opts.
  produce_opts: the keys are :deliver_after_ms and :deliver_at_ms both take an int as the value for :deliver_at_ms the int is a unix timestamp in milliseconds for :deliver_after_ms it's the delay to send the message after in milliseconds
  Uses the global pulsar client for connection information.
  Will return :ok on sucess or an {:error, String.t()} on failure
  """
  @spec async_produce(Genserver.server(), String.t(), map()) :: GenServer.call()
  def async_produce(producer_lookup, msg, produce_opts \\ %{})
      when is_pid(producer_lookup) or is_atom(producer_lookup) or
             (is_tuple(producer_lookup) and is_binary(msg) and is_map(produce_opts)) do
    GenServer.call(producer_lookup, {:async_produce, msg, produce_opts})
  end
end

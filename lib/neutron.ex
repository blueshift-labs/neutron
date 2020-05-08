defmodule Neutron do
  @moduledoc """
  A C nif-based Apache Pulsar Client
  """

  alias Neutron.{PulsarAsyncProducer, PulsarClient, PulsarConsumer, PulsarNifs}

  @doc """
  Register and start Pulsar consumer under the Neutron supervisor tree

  Using a keyword list [callback_module: ModuleName] value.

  The callback needs to implement the PulsarConsumerCallback behaviour (@callback handle_message(String.t()) :: :ack | :ack_all | :nack)

  The callback handle_message function is applied to each message from Pulsar to this consumer.
  The callback handle_message function should return one of these three atoms:

  :ack
  :ack_all
  :nack

  :ack and :ack_all have relatively transparent counterparts in the Pulsar network API.
  However :nack does not, and should be used with great care (it is implemented client-side).

  A callback return value of :nack will cause the Pulsar client to accumulate a data structure of message ids, periodically sending requests to Pulsar with the ids. There is no logic in the consumer to prevent this data structure from growing without bound, and, if the valid messages are not forthcoming, the Pulsar client /should/ run out of memory. As far as I know, this unfortunate flaw is inherent in the current design of Pulsar.

  Another argument in the keyword list is consumer type with the valid types of :exclusive, :shared, :failover, or :key_shared and passed with a [consumer_type: :shared] keyword list value. This defaults to :shared. :shared does not support :ack_all as expected due to the protocol.

  The last keyword list arguments are topic and subscription binaries ("strings"), i.e. [topic: "my-topic", subscription: "my-subscription"]. The default topic is "my-topic" and the default subscription "my-subscription" fallbacks are included.

  """
  @spec start_consumer(Keyword.t()) :: GenServer.on_start()
  def start_consumer(args \\ []) when is_list(args) do
    # in the C/C++ code the default is exclusive this defaults to shared subscription
    Neutron.Application.start_child(PulsarConsumer.child_spec(args))
  end

  @doc """
  Does a synchronous produce to given topic binary with given message binary. Will bootstrap a pulsar producer for each call so async produce where you re-use the producer is more efficient. Uses the global pulsar client for connection information.
  """
  @spec sync_produce(String.t(), String.t()) :: :ok | {:error, String.t()}
  def sync_produce(topic, message) when is_binary(topic) and is_binary(message) do
    {:ok, client_ref} = PulsarClient.get_client()
    PulsarNifs.sync_produce(client_ref, topic, message)
  end

  @doc """
  Creates an asynchronous producer genServer with associated topic and callback. Returns a pid to be managed and re-used by the callie.

  The callback function is applied to each message delivered from Pulsar to this producer and needs to implement the PulsarAsyncProducerCallback behaviour. (@callback handle_delivery({:ok, String.t()} | {:error, String.t()}) :: any())

  The callback function's return value is ignored. It is either passed in {:ok, String.t()} | {:error, String.t()} depending on if the message was successfully produced or if there was an error where the String.t() is the serialized messageId which is comma seperated surrounded by parentheses. This can be re-serialized and passed back to pulsar, if needed:

  int64 ledgerId
  int64 entryId
  int32 partition
  int32 batchIndex
  """
  @spec create_async_producer(String.t(), atom()) :: GenServer.on_start()
  def create_async_producer(topic, callback_module) when is_binary(topic) and is_atom(callback_module) do
    PulsarAsyncProducer.start_link(topic: topic, callback_module: callback_module)
  end

  @doc """
  Does a asynchronous produce using the given producer pid generated from create_async_producer and given message binary. Uses the global pulsar client for connection information. Will always return :ok
  """
  @spec async_produce(pid(), String.t()) :: GenServer.cast()
  def async_produce(producer_pid, msg) when is_pid(producer_pid) and is_binary(msg) do
    GenServer.cast(producer_pid, {:async_produce, msg})
  end
end

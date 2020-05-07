defmodule Neutron do
  @moduledoc """
  A C nif-based Apache Pulsar Client
  """

  alias Neutron.{PulsarAsyncProducer, PulsarClient, PulsarConsumer, PulsarNifs}

  @doc """
  Register and start Pulsar consumer under the Neutron supervisor tree

  Using a keyword list [callback_module: ModuleName] value.

  The callback function is applied to each message from Pulsar to this consumer.
  The callback function should return one of three atoms:

  :ack
  :ack_all
  :nack

  :ack and :ack_all have relatively transparent counterparts in the Pulsar network API.
  However :nack does not, and should be used with great care (it is implemented client-side).

  A callback return value of :nack will cause the Pulsar client to accumulate a data structure of message ids, periodically sending requests to Pulsar with the ids. There is no logic in the consumer to prevent this data structure from growing without bound, and, if the valid messages are not forthcoming, the Pulsar client /should/ run out of memory. As far as I know, this unfortunate flaw is inherent in the current design of Pulsar.

  For consumer type the valid types are :exclusive, :shared, :failover, or :key_shared and passed with a [consumer_type: :shared] keyword list value.

  For topic and subscription binaries ("strings") are expected and the options are passed in the keyword list as well, i.e. [topic: "my-topic", subscription: "my-subscription"]. The default topic "my-topic" and default subscription "my-subscription" are used as fallbacks.

  """
  def start_consumer(args \\ []) when is_list(args) do
    # in the C/C++ code the default is exclusive this defaults to shared subscription
    Neutron.Application.start_child(PulsarConsumer.child_spec(args))
  end

  def sync_produce(topic, message) when is_binary(topic) and is_binary(message) do
    {:ok, client_ref} = PulsarClient.get_client()
    PulsarNifs.sync_produce(client_ref, topic, message)
  end

  def create_async_producer(topic) when is_binary(topic) do
    PulsarAsyncProducer.start_link(topic: topic)
  end

  def async_produce(producer_pid, msg) when is_pid(producer_pid) and is_binary(msg) do
    GenServer.cast(producer_pid, {:async_produce, msg})
  end
end

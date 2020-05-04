defmodule Neutron do
  @moduledoc """
  Documentation for Neutron.
  """

  alias Neutron.{PulsarAsyncProducer, PulsarClient, PulsarConsumer, PulsarNifs}

  # ToDo flesh this out with more args and better API
  def start_consumer(args \\ []) do
    # in the C++ code the default is exclusive this defaults to shared subscription
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

defmodule Neutron do
  @moduledoc """
  Documentation for Neutron.
  """

  alias Neutron.{PulsarClient, PulsarNifs}

  # ToDo flesh this out with more args and better API
  def start_consumer(args \\ []) do
    # in the C++ code the default is exclusive this defaults to shared subscription
    Neutron.Application.start_child(Neutron.PulsarConsumer.child_spec(args))
  end

  def sync_produce(topic, subscription) when is_binary(topic) and is_binary(subscription) do
    {:ok, client_ref} = PulsarClient.get_client()
    PulsarNifs.sync_produce(client_ref, topic, subscription)
  end
end

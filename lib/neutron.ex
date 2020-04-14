defmodule Neutron do
  @moduledoc """
  Documentation for Neutron.
  """

  @on_load :load_nifs

  def load_nifs do
    :erlang.load_nif('./priv/neutron_nif', 0)
  end

  # todo super simple sync produce and then more complex pulsar_producer_send_async
  # which should follow same pattern as the consumer with callback
  def test_produce(client_ref) do
    raise "NIF test_produce/1 not implemented"
  end

  def make_client(config_map) do
    raise "NIF make_client/1 not implemented"
  end

  def destroy_client(client_ref) do
    raise "NIF destroy_client/1 not implemented"
  end

  def do_consume(client_ref, config) do
    raise "NIF do_consume/2 not implemented"
  end

  # need start_client dirty IO
  # stop_client probaby not dirty IO

  # start consumer don't think need dirty IO
  # ack message maybe dirty IO [just do a wrapper with message_id for both pos and neg]
  # stop consumer probably dirty IO

  # consumer_types
  def start_consumer(topic, subscription, config \\ []) do
    # in the C++ code the default is exclusive
    client_ref = Neutron.PulsarClient.get_client()
    Neutron.PulsarConsumer.start(topic, subscription, config)
    # call start for pulsar_consumer with right params which should do the nif stuff
    # pass-in the ref and callback and client, topic, sub
  end
end

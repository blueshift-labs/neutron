defmodule Neutron.PulsarNifs do
  @moduledoc "all the C nifs"
  @on_load :load_nifs

  def load_nifs do
    :erlang.load_nif('./priv/neutron_nif', 0)
  end

  def sync_produce(client_ref, topic, message, produce_opts) do
    raise "NIF sync_produce/4 not implemented"
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

  def ack(consumer_ref, message_id) do
    raise "NIF ack/2 not implemented"
  end

  def ack_all(consumer_ref, message_id) do
    raise "NIF ack/2 not implemented"
  end

  def nack(consumer_ref, message_id) do
    raise "NIF nack/2 not implemented"
  end

  def destroy_consumer(consumer_ref) do
    raise "NIF destroy_consumer/1 not implemented"
  end

  def create_async_producer(client_ref, topic, callback_pid) do
    raise "NIF create_async_producer/3 not implemented"
  end

  def async_produce(producer_ref, msg, produce_opts) do
    raise "NIF async_produce/3 not implemented"
  end

  def destroy_producer(producer_ref) do
    raise "NIF destroy_producer/1 not implemented"
  end
end

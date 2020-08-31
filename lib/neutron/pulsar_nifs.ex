defmodule Neutron.PulsarNifs do
  @moduledoc "all the C nifs"
  @on_load :load_nifs

  def load_nifs do
    directory =
      case :code.priv_dir(:neutron) do
        {:error, :bad_name} ->
          ebin = :filename.dirname(:code.which(__MODULE__))
          :filename.join([:filename.dirname(ebin), "priv", "neutron_nif"])

        dir ->
          :filename.join(dir, "neutron_nif")
      end

    :erlang.load_nif(directory, 0)
  end

  def sync_produce(_client_ref, _topic, _message, _produce_opts) do
    raise "NIF sync_produce/4 not implemented"
  end

  def make_client(_config_map) do
    raise "NIF make_client/1 not implemented"
  end

  def destroy_client(_client_ref) do
    raise "NIF destroy_client/1 not implemented"
  end

  def do_consume(_client_ref, _config) do
    raise "NIF do_consume/2 not implemented"
  end

  def ack(_consumer_ref, _message_id) do
    raise "NIF ack/2 not implemented"
  end

  def ack_all(_consumer_ref, _message_id) do
    raise "NIF ack/2 not implemented"
  end

  def nack(_consumer_ref, _message_id) do
    raise "NIF nack/2 not implemented"
  end

  def destroy_consumer(_consumer_ref) do
    raise "NIF destroy_consumer/1 not implemented"
  end

  def create_async_producer(_client_ref, _topic, _callback_pid) do
    raise "NIF create_async_producer/3 not implemented"
  end

  def async_produce(_producer_ref, _msg, _produce_opts) do
    raise "NIF async_produce/3 not implemented"
  end

  def destroy_producer(_producer_ref) do
    raise "NIF destroy_producer/1 not implemented"
  end
end

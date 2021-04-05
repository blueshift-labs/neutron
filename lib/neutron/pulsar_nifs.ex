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

  def create_client(_url, _opts) do
    raise "NIF create_client/2 not implemented"
  end

  def destroy_client(_client_ref) do
    raise "NIF destroy_client/1 not implemented"
  end

  def create_consumer(_client_ref, _topic, _subscription, _callback_pid, _options) do
    raise "NIF do_consume/5 not implemented"
  end

  def ack(_consumer_ref, _message_id) do
    raise "NIF ack/2 not implemented"
  end

  def nack(_consumer_ref, _message_id) do
    raise "NIF nack/2 not implemented"
  end

  def destroy_consumer(_consumer_ref) do
    raise "NIF destroy_consumer/1 not implemented"
  end

  def create_producer(_client_ref, _topic, _options) do
    raise "NIF create_async_producer/3 not implemented"
  end

  def produce(_producer_ref, _msg, _produce_opts) do
    raise "NIF produce/3 not implemented"
  end

  def destroy_producer(_producer_ref) do
    raise "NIF destroy_producer/1 not implemented"
  end
end

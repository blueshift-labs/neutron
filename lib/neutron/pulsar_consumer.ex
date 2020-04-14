defmodule Neutron.PulsarConsumer do
  @moduledoc false

  use GenServer

  alias Neutron.PulsarClient

  # ToDo flesh this out
  # def start(topic, subscription, config) do
  #   # todo make child_spec
  #   Neutron.Application.start_child(%{})
  # end

  def start_link(arg) do
    callback_module = Keyword.get(arg, :topic, Test)

    if !is_list(callback_module.module_info[:attributes][:behaviour]) and
         !Enum.member?(
           callback_module.module_info[:attributes][:behaviour],
           Neutron.PulsarConsumerCallback
         ) do
      raise "error you need to implement the Neutron.PulsarConsumerCallback on your consumer"
    end

    subscription = Keyword.get(arg, :subscription, "my-subscription")
    topic = Keyword.get(arg, :topic, "my-topic")

    GenServer.start_link(__MODULE__, %{
      topic: topic,
      subscription: subscription,
      callback_module: callback_module
    })
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    {:ok, client_ref} = PulsarClient.get_client()
    # todo make these passed-in and maybe add-in consumer_name
    full_config = Map.put(config, :send_back_to_pid, self())
    {:ok, consumer_ref} = Neutron.do_consume(client_ref, full_config)
    {:ok, %{config: config, consumer_ref: consumer_ref}}
  end

  @impl true
  def handle_info(
        {:listener_callback, msg, msg_id_ref},
        %{config: %{callback_module: callback_module}, consumer_ref: consumer_ref} = state
      ) do
    # IO.inspect("received a message #{msg} with msg_id_ref #{msg_id_ref}")

    res =
      try do
        callback_module.handle_message(msg)
      catch
        _any -> {:error, :exception}
      end

    case res do
      {:ok, _any} ->
        :ok = Neutron.ack(consumer_ref, msg_id_ref)
      {:error, _any} ->
        :ok = Neutron.nack(consumer_ref, msg_id_ref)
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{consumer_ref: consumer_ref} = state) do
    :ok = Neutron.destroy_consumer(consumer_ref)

    state
  end
end

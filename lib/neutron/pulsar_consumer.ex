defmodule Neutron.PulsarConsumer do
  @moduledoc false

  use GenServer

  alias Neutron.{PulsarClient, PulsarNifs}

  def child_spec(arg \\ []) do
    %{
      id: :"PulsarConsumer-#{:erlang.unique_integer([:monotonic])}",
      start: {Neutron.PulsarConsumer, :start_link, [arg]}
    }
  end

  def start_link(arg) do
    callback_module = Keyword.get(arg, :callback_module, Test)

    behaviours =
      callback_module.module_info[:attributes]
      |> Keyword.pop_values(:behaviour)
      |> elem(0)
      |> List.flatten()

    if !Enum.member?(behaviours, Neutron.PulsarConsumerCallback) do
      raise "error you need to implement the Neutron.PulsarConsumerCallback for your consumer"
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
    {:ok, consumer_ref} = PulsarNifs.do_consume(client_ref, full_config)
    {:ok, %{config: config, consumer_ref: consumer_ref}}
  end

  @impl true
  def handle_info(
        {:listener_callback, msg, msg_id_ref},
        %{config: %{callback_module: callback_module}, consumer_ref: consumer_ref} = state
      ) do
    res =
      try do
        callback_module.handle_message(msg)
      catch
        _any -> {:error, :exception}
      end

    case res do
      {:ok, _any} ->
        :ok = PulsarNifs.ack(consumer_ref, msg_id_ref)

      {:error, _any} ->
        :ok = PulsarNifs.nack(consumer_ref, msg_id_ref)
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{consumer_ref: consumer_ref} = state) do
    :ok = PulsarNifs.destroy_consumer(consumer_ref)

    state
  end
end

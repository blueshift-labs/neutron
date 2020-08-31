defmodule Neutron.PulsarConsumer do
  @moduledoc false

  use GenServer

  alias Neutron.{PulsarClient, PulsarNifs}

  def child_spec(arg \\ []) do
    %{
      id: :"PulsarConsumer-#{:erlang.unique_integer([:monotonic])}",
      start: {Neutron.PulsarConsumer, :start_link, [arg]},
      shutdown: :infinity
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
    consumer_type = Keyword.get(arg, :consumer_type, :shared)

    if !is_binary(topic) || !is_binary(subscription) do
      raise "invalid arg given. Please pass-in a String.t()"
    end

    GenServer.start_link(__MODULE__, %{
      topic: topic,
      subscription: subscription,
      callback_module: callback_module,
      consumer_type: consumer_type
    })
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    {:ok, consumer_ref} = create_consumer(config)
    {:ok, %{config: config, consumer_ref: consumer_ref}}
  end

  @impl true
  def handle_info(
        {:listener_callback, msg, msg_id_ref},
        %{config: %{callback_module: callback_module}, consumer_ref: consumer_ref} = state
      ) do
    res =
      try do
        # todo investigate the bug that sometimes causes a unicode character from C library with pulsar
        callback_module.handle_message(
          String.trim_trailing(msg, "\a"),
          state
        )      
      catch
        _any -> {:error, :exception}
      end

    case res do
      :ack ->
        :ok = PulsarNifs.ack(consumer_ref, msg_id_ref)

      :ack_all ->
        :ok = PulsarNifs.ack_all(consumer_ref, msg_id_ref)

      :nack ->
        :ok = PulsarNifs.nack(consumer_ref, msg_id_ref)
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{consumer_ref: consumer_ref} = state) do
    :ok = PulsarNifs.destroy_consumer(consumer_ref)

    state
  end

  defp create_consumer(%{consumer_type: consumer_type} = config) do
    {:ok, client_ref} = PulsarClient.get_client()

    consumer_enum_int =
      case consumer_type do
        :exclusive ->
          0

        :shared ->
          1

        :failover ->
          2

        :key_shared ->
          3

        _ ->
          raise "Invalid :consumer_type specified. Valid types are :exclusive :shared :failover or :key_shared"
      end

    full_config =
      config
      |> Map.put(:type_int, consumer_enum_int)
      |> Map.put(:send_back_to_pid, self())

    PulsarNifs.do_consume(client_ref, full_config)
  end
end

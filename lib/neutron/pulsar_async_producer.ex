defmodule Neutron.PulsarAsyncProducer do
  @moduledoc false

  use GenServer

  alias Neutron.{PulsarClient, PulsarNifs}

  def child_spec(arg \\ []) do
    %{
      id: :"PulsarAsyncProducer-#{:erlang.unique_integer([:monotonic])}",
      start: {Neutron.PulsarAsyncProducer, :start_link, [arg]},
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

    if !Enum.member?(behaviours, Neutron.PulsarAsyncProducerCallback) do
      raise "error you need to implement the Neutron.PulsarAsyncProducerCallback for your producer"
    end

    topic = Keyword.get(arg, :topic, "my-topic")

    GenServer.start_link(__MODULE__, %{
      topic: topic,
      callback_module: callback_module
    })
  end

  @impl true
  def init(%{topic: topic} = config) do
    Process.flag(:trap_exit, true)
    {:ok, client_ref} = PulsarClient.get_client()
    {:ok, producer_ref} = PulsarNifs.create_async_producer(client_ref, topic, self())
    {:ok, %{config: config, producer_ref: producer_ref}}
  end

  @impl true
  def handle_info(
        {:delivery_callback, result, msg_id},
        %{config: %{callback_module: callback_module}} = state
      ) do
    try do
      callback_module.handle_delivery({result, msg_id})
    catch
      _any -> {:error, :exception}
    end

    {:noreply, state}
  end

  @impl true
  def handle_call({:async_produce, msg}, _from, %{producer_ref: producer_ref} = state) do
    res = PulsarNifs.async_produce(producer_ref, msg)

    {:reply, res, state}
  end

  @impl true
  def terminate(_reason, %{producer_ref: producer_ref} = state) do
    :ok = PulsarNifs.destroy_producer(producer_ref)

    state
  end
end

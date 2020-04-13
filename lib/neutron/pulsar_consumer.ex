defmodule Neutron.PulsarConsumer do
  @moduledoc false

  use GenServer

  def start(topic, subscription, config) do
    # todo make child_spec
    Neutron.Application.start_child(%{})
  end

  def start_link(_arg) do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl true
  def init(%{config: config, consumer_ref: :empty} = state) do
    Process.flag(:trap_exit, true)

    subscription_type = Keyword.get(state, :consumer_type, :shared)
    # grab callback module, topic, sub, and maybe consumerName
    # call nif here to make consumer and populate state with ref and callback
    {:ok, {}}
  end

  @impl true
  def handle_cast(:test, state) do
    {:noreply, {}}
  end

  @impl true
  def terminate(_reason, state) do
    # todo call nif here to destroy consumer

    state
  end

end

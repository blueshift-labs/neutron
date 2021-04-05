defmodule Neutron.Application do
  @moduledoc false

  alias Neutron.PulsarClient

  use Application

  def start(_type, _args) do
    PulsarClient.start_client()

    children = [
      {Registry,
       id: Neutron.ProducerRegistry,
       start:
         {Registry, :start_link, [keys: :unique, name: Neutron.ProducerRegistry],
          partitions: System.schedulers_online()},
       shutdown: :infinity}
      # {DynamicSupervisor,
      #  strategy: :one_for_one,
      #  name: Neutron.ProducerSupervisor,
      #  max_restarts: 2_000,
      #  shutdown: :infinity,
      #  max_seconds: 1}
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Neutron.Supervisor,
      shutdown: :infinity
    )
  end

  def stop(_state) do
    PulsarClient.stop_client()
  end
end

defmodule Neutron.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  alias Neutron.PulsarClient

  use Application

  @dynamic_supervisor Neutron.DynamicSupervisor

  def start(_type, _args) do
    children = [
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: @dynamic_supervisor,
       max_restarts: 2_000,
       shutdown: :infinity,
       max_seconds: 1}
    ]

    opts = [strategy: :one_for_one, name: Neutron.Supervisor, shutdown: :infinity]

    PulsarClient.start_client()

    Supervisor.start_link(children, opts)
  end

  def stop(_state) do
    DynamicSupervisor.stop(@dynamic_supervisor)
    PulsarClient.stop_client()
  end

  def start_child(child_spec) do
    DynamicSupervisor.start_child(@dynamic_supervisor, child_spec)
  end

  def terminate_child(child_pid) do
    :ok = DynamicSupervisor.terminate_child(@dynamic_supervisor, child_pid)
  end
end

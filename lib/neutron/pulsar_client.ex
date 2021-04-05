defmodule Neutron.PulsarClient do
  @moduledoc "pulsar global client using persistent term API"

  alias Neutron.PulsarNifs

  @client_key :pulsar_client

  def start_client do
    url = Application.fetch_env!(:neutron, :url)
    thread_count_to_use = Application.get_env(:neutron, :thread_count, System.schedulers_online())

    client_ref =
      PulsarNifs.create_client(
        url,
        Application.get_env(:neutron, :client_opts, []) |> Enum.into(%{})
      )

    :persistent_term.put(@client_key, client_ref)
  end

  def get_client do
    :persistent_term.get(@client_key)
  end

  def stop_client do
    client_ref = :persistent_term.get(@client_key)
    :ok = PulsarNifs.destroy_client(client_ref)
    :persistent_term.erase(@client_key)
  end
end

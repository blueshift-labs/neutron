defmodule Neutron.PulsarClient do
  @moduledoc false

  alias Neutron.PulsarNifs

  @client_key :pulsar_client

  def start_client do
    # todo make these possible to pass-in with function call
    pulsar_url = Application.get_env(:neutron, :url, "pulsar://localhost:6650")
    thread_count_to_use = System.schedulers_online()

    client_ref =
      PulsarNifs.make_client(%{
        io_threads: thread_count_to_use,
        msg_listener_threads: thread_count_to_use,
        url: pulsar_url
      })

    :persistent_term.put(@client_key, client_ref)
  end

  def get_client() do
    :persistent_term.get(@client_key)
  end

  def stop_client do
    client_ref = :persistent_term.get(@client_key)
    :ok = PulsarNifs.destroy_client(client_ref)
    :persistent_term.erase(@client_key)
  end
end

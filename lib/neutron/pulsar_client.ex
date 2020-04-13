defmodule Neutron.PulsarClient do
  @moduledoc false

  @client_key :pulsar_client

  def start_client do
    # todo make this possible to pass-in with function call
    pulsar_url = Application.get_env(:neutron, :url, "pulsar://localhost:6650")
    thread_count_to_use = System.schedulers_online()
    client_ref = Neutron.make_client(%{io_threads: thread_count_to_use, msg_listener_threads: thread_count_to_use, url: pulsar_url})
    # need a wrapping struct and then return a ref/resource with struct
    :persistent_term.put(@client_key, client_ref)
  end

  def get_client() do
    :persistent_term.get(@client_key)
  end

  def stop_client do
    ref = :persistent_term.get(@client_key)
    # call nif stuff to close, free client and config
    :todo_nif_cleanup_call
    :persistent_term.erase(@client_key)
  end

end

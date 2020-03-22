defmodule Neutron do
  @moduledoc """
  Documentation for Neutron.
  """
  @on_load :load_nifs

  def load_nifs do
    :erlang.load_nif('./priv/neutron_nif', 0)
  end

  def test(_string) do
    raise "NIF test/1 not implemented"
  end
end

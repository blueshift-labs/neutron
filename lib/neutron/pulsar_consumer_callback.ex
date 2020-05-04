defmodule Neutron.PulsarConsumerCallback do
  @callback handle_message(String.t()) :: {:ok, any()} | {:error, any()}
end

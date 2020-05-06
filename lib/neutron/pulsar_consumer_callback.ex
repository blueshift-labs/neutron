defmodule Neutron.PulsarConsumerCallback do
  @callback handle_message(String.t()) :: {:ok, any()} | {:ack_all, any()} | {:error, any()}
end

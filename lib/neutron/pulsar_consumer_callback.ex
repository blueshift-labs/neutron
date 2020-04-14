defmodule Neutron.PulsarConsumerCallback do
  # ToDo put in proper types for message
  @callback handle_message(String.t()) :: {:ok, any()} | {:error, any()}
end

defmodule Neutron.PulsarConsumerCallback do
  # ToDo put in proper types for message
  @callback handle_message(any()) :: {:ok, any()} | {:error, any()}
end
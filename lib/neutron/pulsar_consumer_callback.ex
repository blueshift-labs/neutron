defmodule Neutron.PulsarConsumerCallback do
  @callback handle_message(String.t()) :: :ack | :ack_all | :nack
end

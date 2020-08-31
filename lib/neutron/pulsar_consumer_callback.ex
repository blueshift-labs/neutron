defmodule Neutron.PulsarConsumerCallback do
  @moduledoc "callback for consumers to implement for handling a message and whether to ack it"
  @callback handle_message(String.t(), any()) :: :ack | :ack_all | :nack
end

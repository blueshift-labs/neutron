defmodule Neutron.PulsarConsumerCallback do
  @type properties :: [{String.t(), String.t()}]
  @type state :: any()

  @moduledoc "callback for consumers to implement for handling a message and whether to ack it"
  @callback handle_message(
    {:neutron_msg, String.t(), reference(), String.t(), pos_integer(), pos_integer(), non_neg_integer(), properties(), String.t()},
    state()
  ) :: :ack | :ack_all | :nack
end

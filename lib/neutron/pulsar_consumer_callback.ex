defmodule Neutron.PulsarConsumerCallback do
  @type properties :: [{String.t(), String.t()}] | %{String.t() => String.t()}
  @type state :: any()

  @moduledoc """
  Callback for consumers to implement for handling a message and whether to ack it.
  The message contains following elements:
    {:neutron_msg, topic, msg_id_ref, partition_key, publish_ts, event_ts, redelivery_count, properties, payload}
  """
  @callback handle_message(
              {:neutron_msg, String.t(), reference(), String.t(), pos_integer(), pos_integer(),
               non_neg_integer(), properties(), String.t()},
              state()
            ) :: :ack | :ack_all | :nack
end

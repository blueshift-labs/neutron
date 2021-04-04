defmodule Neutron.PulsarAsyncProducerCallback do
  @moduledoc "Callback for async producer's delivery status"
  # the string is the serialized msg_id
  # int64 ledgerId
  # int64 entryId
  # int32 partition
  # int32 batchIndex
  @callback handle_delivery({:ok, any(), any(), any()} | {:error, any(), any(), any()}) ::
              any()
end

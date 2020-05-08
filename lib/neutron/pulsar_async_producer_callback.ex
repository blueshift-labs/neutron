defmodule Neutron.PulsarAsyncProducerCallback do
  @moduledoc "callback for async producer's delivery status"
  # the string is the serialized msg_id
  # const int64_t ledgerId_;
  # const int64_t entryId_;
  # const int32_t partition_;
  # const int32_t batchIndex_;
  @callback handle_delivery({:ok, String.t()} | {:error, String.t()}) :: any()
end

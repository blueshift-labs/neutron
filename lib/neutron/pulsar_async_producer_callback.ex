defmodule Neutron.PulsarAsyncProducerCallback do
  @callback handle_delivery({:ok, String.t()} | {:error, String.t()}) :: any()
end

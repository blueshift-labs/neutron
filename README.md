# Neutron

- A simple apache pulsar client using their C API (2.6.0) and nifs.
- The persistent_term API is being used so this requires OTP 21+ hence elixir 1.10+.
- Caution `ack_all` doesn't work as expected with `shared` subscription.

## Installation (this might go to hex eventually)

```elixir
def deps do
  [
    {:neutron, github: "IRog/neutron"}
  ]
end
```

### Configure
In your configs:

```elixir
config :neutron,
  url: "pulsar://localhost:6650", # this is the default value and ideally should be set
  thread_count: 4 # this is optional and defaults to System.schedulers_online()
```

The above is used for the pulsar client configuration. There is 1 global client that's stored using persistent_term. The thread count is used for the pulsar client IO Threads and Message Listener Threads


### Consuming
How to start a consumer under neutron's supervisor tree:

```elixir
defmodule ConsumerCallback do
  @behaviour Neutron.PulsarConsumerCallback

  @impl true
  def handle_message(msg) do
    IO.inspect(msg)
    :ack
  end
end

Neutron.start_consumer([callback_module: ConsumerCallback, consumer_type: :shared, topic: "my-topic", subscription: "my-subscription"])
```

See the `start_consumer` documentation for more information. The above shows all the default values being used minus callback_module which defaults to a module which shouldn't exist and is required.

### Producing
How to do an async produce. Ideally the producer created by the first call should be put under a supervisor tree or threaded through your program. The sync produce API is also available but is also less efficient as a producer is created in the nif per call

```elixir
defmodule DeliverCallback do
  @behaviour Neutron.PulsarAsyncProducerCallback

  @impl true
  def handle_delivery(res) do
    case res do
      {:ok, _msg_id_string} -> IO.inspect "successful produce"
      {:error, _msg_id_string} -> IO.inspect "failed produce"
    end
  end
end

# async produce
{:ok, pid} = Neutron.create_async_producer("my-topic", DeliverCallback)
:ok = Neutron.async_produce(pid, "hello message")

# sync produce
:ok = Neutron.sync_produce("my-topic", "hello message redux")
```

Compression is still not supported for producing but will likely be added in a later version.
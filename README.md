# Neutron

- A simple apache pulsar client using their C API and nifs. Right now it's config-lite and uses the number of schedulers for the listener and io threads and 1 global client.
- The persistent_term API is being used so this requires OTP 21+ hence elixir 1.10+.
- Caution `ack_all` doesn't work as expected with `shared` subscription.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `neutron` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:neutron, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/neutron](https://hexdocs.pm/neutron).


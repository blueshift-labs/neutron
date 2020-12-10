import Config

# Print only warnings and errors during test
config :logger, level: :info

config :neutron,
  divo: [
    {DivoPulsar, [port: 8080, version: "2.7.0"]}
  ],
  divo_wait: [dwell: 10_000, max_tries: 50]

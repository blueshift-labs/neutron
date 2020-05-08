import Config

# Print only warnings and errors during test
config :logger, level: :info

config :neutron,
  divo: "test/support/docker-compose.yaml",
  divo_wait: [dwell: 700, max_tries: 50]

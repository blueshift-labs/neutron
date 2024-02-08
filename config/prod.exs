import Config

# Do not print debug/info messages in production
config :logger,
  level: :warning,
  compile_time_purge_matching: [
    [level_lower_than: :warning]
  ]

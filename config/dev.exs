import Config
# For development, we disable any cache and enable
# debugging and code reloading.
#
# The watchers configuration can be used to run external
# watchers to your application. For example, we use it
# with webpack to recompile .js and .css sources.
#
config :neutron,
  url: "pulsar://localhost:6650",
  client_opts: []

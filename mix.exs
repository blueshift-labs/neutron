defmodule Neutron.MixProject do
  use Mix.Project

  def project do
    [
      app: :neutron,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_clean: ["clean"],
      make_env: make_env(),
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_paths: test_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Neutron.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:elixir_make, "~> 0.6", runtime: false},
      {:credo, "~> 1.4", only: [:dev], runtime: false},
      {:divo, "~> 1.1.9", only: [:dev, :integration]},
      {:divo_pulsar, "~> 0.1.0", only: [:dev, :integration]},
      {:stream_data, "~> 0.1", only: [:test, :integration]}
    ]
  end

  defp make_env do
    ebin = :filename.dirname(:code.which(__MODULE__))
    priv_dir = to_string(:filename.join([:filename.dirname(ebin), "priv", "neutron_nif"]))

    IO.inspect(priv_dir)
    IO.inspect(System.cmd("ls", []))
    IO.inspect(System.cmd("pwd", []))
    IO.inspect("-----------------------------")

    %{
      "ERL_EI_INCLUDE_DIR" =>
        System.get_env("ERL_EI_INCLUDE_DIR") ||
          "#{:code.root_dir()}/erts-#{:erlang.system_info(:version)}/include",
      "ERL_EI_LIBDIR" =>
        System.get_env("ERL_EI_LIBDIR") ||
          "#{:code.root_dir()}/erts-#{:erlang.system_info(:version)}/lib",
      "PRIV_DIR" => priv_dir
    }
  end

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]
end

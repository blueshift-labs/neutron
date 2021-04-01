defmodule Neutron.MixProject do
  use Mix.Project

  def project do
    [
      app: :neutron,
      compilers: [:elixir_make] ++ Mix.compilers(),
      build_embedded: Mix.env() == :prod,
      make_clean: ["clean"],
      version: "0.2.1",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_paths: test_paths(Mix.env()),
      package: package(),
      description: "Elixir client for Apache Pulsar, uses CPP client"
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
      {:credo, "~> 1.5", only: [:dev], runtime: false},
      {:divo, "~> 1.3", only: [:dev, :integration], override: true},
      {:divo_pulsar, "~> 0.1.1", only: [:dev, :integration]},
      {:stream_data, "~> 0.5", only: [:test, :integration]},
      {:elixir_uuid, "~> 1.2", only: [:dev, :integration]},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false}
    ]
  end

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp package() do
    [
      links: %{
        "GitHub" => "https://github.com/blueshift-labs/neutron"
      },
      licenses: ["Apache-2.0"],
      source_url: "https://github.com/blueshift-labs/neutron",
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "c_src",
        "Makefile",
        "build_deps.sh"
      ]
    ]
  end
end

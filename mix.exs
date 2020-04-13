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
      deps: deps()
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
      {:elixir_make, "~> 0.6", runtime: false}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  defp make_env do
    %{
      "ERL_EI_INCLUDE_DIR" =>
        System.get_env("ERL_EI_INCLUDE_DIR") || Path.join([:code.root_dir(), "usr", "include"]),
      "ERL_EI_LIBDIR" =>
        System.get_env("ERL_EI_LIBDIR") || Path.join([:code.root_dir(), "usr", "lib"])
    }
  end
end

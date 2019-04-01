defmodule ElixirDruid.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_druid,
      version: "1.0.0",
      elixir: "~> 1.8.1",
      start_permanent: Mix.env() == :prod,
      description: "Client library for sending requests to Druid.",
      source_url: "https://github.com/gameanalytics/elixir_druid",
      package: package(),
      deps: deps(),

      # Docs
      name: "ElixirDruid",
      source_url: "https://github.com/GameAnalytics/elixir_druid",
      homepage_url: "https://github.com/GameAnalytics/elixir_druid",
      docs: [
        main: "ElixirDruid", # The main page in the docs
        extras: ["README.md"]
      ]
    ]
  end

  defp package do
    [
      files: ["config", "lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Magnus Henoch"],
      licenses: ["Apache-2.0"],
      links: %{github: "https://github.com/gameanalytics/elixir_druid"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.1"},
      {:httpoison, "~> 1.0"},
      {:timex, "~> 3.1"},
      {:dialyxir, "~> 1.0-rc.3", only: [:dev], runtime: false},
      {:credo, "~> 0.9.3", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.19.3", only: :dev, runtime: false},
    ]
  end
end

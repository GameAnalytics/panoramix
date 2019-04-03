defmodule Panoramix.MixProject do
  use Mix.Project

  def project do
    [
      app: :panoramix,
      version: System.cmd("git", ["describe", "--tags"]) |> elem(0) |> String.trim_trailing |> String.trim_leading("v"),
      elixir: "~> 1.8.1",
      start_permanent: Mix.env() == :prod,
      description: "Client library for sending requests to Druid.",
      source_url: "https://github.com/GameAnalytics/panoramix",
      package: package(),
      deps: deps(),

      # Docs
      name: "Panoramix",
      source_url: "https://github.com/GameAnalytics/panoramix",
      homepage_url: "https://github.com/GameAnalytics/panoramix",
      docs: [
        main: "Panoramix", # The main page in the docs
        extras: ["README.md"]
      ]
    ]
  end

  defp package do
    [
      files: ["config", "lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Magnus Henoch"],
      licenses: ["Apache-2.0"],
      links: %{github: "https://github.com/GameAnalytics/panoramix"}
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

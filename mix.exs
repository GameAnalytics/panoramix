defmodule ElixirDruid.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_druid,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:credo, "~> 0.9.3", only: [:dev, :test], runtime: false}
    ]
  end
end

defmodule OffBroadwayKafka.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_kafka,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env())
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
      {:elsa, path: "../elsa"},
      {:broadway, "~> 0.3.0"},
      {:placebo, "~> 1.2", only: [:dev, :test, :integration]},
      {:checkov, "~> 0.4.0", only: [:dev, :test, :integration]},
      {:divo, "~> 1.1", only: [:dev, :integration]},
      {:divo_kafka, "~> 0.1.4", only: [:dev, :integration]},
      {:patiently, "~> 0.2.0", only: [:test, :integration], override: true}
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]
end

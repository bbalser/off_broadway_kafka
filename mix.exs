defmodule OffBroadwayKafka.MixProject do
  use Mix.Project

  @github "https://github.com/bbalser/off_broadway_kafka"

  def project do
    [
      app: :off_broadway_kafka,
      version: "0.5.1",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      source_url: @github,
      homepage_url: @github,
      docs: docs(),
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
      {:elsa, "~> 0.11.1"},
      {:broadway, "~> 0.5.0"},
      {:retry, "~> 0.13.0"},
      {:placebo, "~> 1.2", only: [:dev, :test, :integration]},
      {:checkov, "~> 0.5.0", only: [:dev, :test, :integration]},
      {:divo, "~> 1.1", only: [:dev, :integration]},
      {:divo_kafka, "~> 0.1.6", only: [:dev, :integration]},
      {:patiently, "~> 0.2.0", only: [:test, :integration], override: true},
      {:ex_doc, "~> 0.21.2", only: [:dev], runtime: false},
      {:benchee, "~> 1.0", only: [:integration]},
      {:dialyxir, "~> 0.5", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Brian Balser", "Jeff Grunewald"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp docs do
    [
      source_url: @github,
      extras: ["README.md"],
      # api_reference: false,
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end

  defp description do
    "Implementation of Broadway that supports a Kafka producer"
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]
end

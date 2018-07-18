defmodule ElixirDruid do

  @moduledoc """
  Documentation for ElixirDruid.
  """

  def post_query(profile, query) do
    broker_profiles = Application.get_env(:elixir_druid, :broker_profiles)
    broker_profile = broker_profiles[profile]
    url = broker_profile[:base_url] <> "/druid/v2"

    options = http_options(url, broker_profile)

    body = ElixirDruid.Query.to_json query
    headers = [{"Content-Type", "application/json"}]

    HTTPoison.post! url, body, headers, options
  end

  def status(profile) do
    broker_profiles = Application.get_env(:elixir_druid, :broker_profiles)
    broker_profile = broker_profiles[profile]
    url = broker_profile[:base_url] <> "/status"

    options = http_options(url, broker_profile)
    headers = []

    case HTTPoison.get! url, headers, options do
      %HTTPoison.Response{
	status_code: 200,
	body: body} ->
	Jason.decode! body
    end
  end

  defp http_options(url, broker_profile) do
    ssl_options(url, broker_profile) ++ auth_options(broker_profile)
  end

  defp ssl_options(url, broker_profile) do
    if url =~ ~r(^https://) do
      cacertfile = broker_profile[:cacertfile]
      [ssl: [verify: :verify_peer, cacertfile: cacertfile]]
    else
      []
    end
  end

  defp auth_options(broker_profile) do
    if broker_profile[:http_username] do
      auth = {broker_profile[:http_username], broker_profile[:http_password]}
      [hackney: [basic_auth: auth]]
    else
      []
    end
  end

end

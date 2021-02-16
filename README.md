# Panoramix

[![Build Status](https://travis-ci.org/GameAnalytics/panoramix.svg?branch=master)](https://travis-ci.org/GameAnalytics/panoramix)

An open-source client library for sending requests to [Apache Druid][druid] from applications written in Elixir. The project uses [HTTPoison][httpoison] as an HTTP client for sending queries.

[druid]: http://druid.io/
[httpoison]: https://github.com/edgurgel/httpoison

## Getting Started

Add Panoramix as a dependency to your project.

```elixir
defp deps do
  [
    {:panoramix, ">= 0.12.0 and < 1.0.0"}
  ]
end
```

## Configuration 

Panoramix requires a Druid Broker profile to be defined in the configuration of your application.

```elixir
config :panoramix,
  request_timeout: 120_000,
  query_priority:  0,
  broker_profiles: [
    default: [
      base_url:       "https://druid-broker-host:9088",
      cacertfile:     "path/to/druid-certificate.crt",
      http_username:  "username",
      http_password:  "password"
    ]
  ]
```

* `request_timeout`: Query timeout in millis to be used in [`Context`](context-druid-doc-link) of all Druid queries. 
* `query_priority`: Priority to be used in [`Context`](context-druid-doc-link) of all Druid queries. 

[context-druid-doc-link]: http://druid.io/docs/latest/querying/query-context.html

The `cacertfile` option in the broker profile names a file that contains the CA certificate for the Druid broker. Alternatively you can specify the certificate as a string in PEM format (starting with `-----BEGIN CERTIFICATE-----`) in the `cacert` option.

## Usage

Build a query like this:

```elixir
use Panoramix

q = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2019-03-01T00:00:00+00:00/2019-03-04T00:00:00+00:00"],
      granularity: :day,
      filter: dimensions.foo == "bar",
       aggregations: [event_count: count(), 
                      unique_id_count: hyperUnique(:user_unique)]  
```

And then send it:

```elixir
Panoramix.post_query(q, :default)
```

Where `:default` is a configuration profile pointing to your Druid server.

The default value for the profile argument is `:default`, so if you
only need a single configuration you can omit it:

```elixir
Panoramix.post_query(q)
```

Response example:
```elixir
{:ok,
 [
   %{
     "result" => %{
       "event_count" => 7544,
       "unique_id_count" => 43.18210933535
     },
     "timestamp" => "2019-03-01T00:00:00.000Z"
   },
   %{
     "result" => %{
       "event_count" => 1051,
       "unique_id_count" => 104.02052398847
     },
     "timestamp" => "2019-03-02T00:00:00.000Z"
   },
   %{
     "result" => %{
       "event_count" => 4591,
       "unique_id_count" => 79.19885795313
     },
     "timestamp" => "2019-03-03T00:00:00.000Z"
   }
 ]}
```

To make a nested query, pass a map of the form `%{type: :query, query: inner_query}`
as data source. For example:

```elixir
use Panoramix

inner_query = from "my_datasource",
                query_type: "topN",
                intervals: ["2019-03-01T00:00:00+00:00/2019-03-04T00:00:00+00:00"],
                granularity: :day,
                aggregations: [event_count: count()],
                dimension: "foo",
                metric: "event_count",
                threshold: 100
q = from %{type: :query, query: inner_query},
      query_type: "timeseries",
      intervals: ["2019-03-01T00:00:00+00:00/2019-03-04T00:00:00+00:00"],
      granularity: :day,
      aggregations: [foo_count: count(),
                     event_count_sum: longSum(:event_count)],
      post_aggregations: [mean_events_per_foo: aggregations.event_count_sum / aggregations.foo_count]
```

To make a join query, pass a map of the form `%{type: :join, left: left, right: right,
joinType: :INNER | :LEFT, rightPrefix: "prefix_", condition: "condition"}`. Both the left
and the right side can be a nested query as above, `%{type: :query, query: inner_query}`,
which will be expanded. Other join sources will be passed to Druid unchanged. For example:

```elixir
use Panoramix

from %{type: :join,
       left: "sales",
       right: %{type: :lookup, lookup: "store_to_country"},
       rightPrefix: "r.",
       condition: "store == \"r.k\"",
       joinType: :INNER},
  query_type: "groupBy",
  intervals: ["0000/3000"],
  granularity: "all",
  dimensions: [%{type: "default", outputName: "country", dimension: "r.v"}],
  aggregations: [country_revenue: longSum(:revenue)]
```

You can also build a JSON query yourself by passing it as a map to
`post_query`:

```elixir
Panoramix.post_query(%{queryType: "timeBoundary", dataSource: "my_datasource"})
```

## Troubleshooting

You can check correctness of your configuration by requesting status from Druid Broker. A successfull response will look like this.

```elixir
iex(1)> Panoramix.status(:default)
{:ok,
 %{
   "memory" => %{...},
   "modules" => [...],
   "version" => "0.13.0"
 }}
```

## Contributions
We'd love to accept your contributions in a form of patches, bug reports and new features! 

Before opening a pull request please make sure your changes pass all the tests. 

## License
Except as otherwise noted this software is licensed under the [Apache License, Version 2.0]((http://www.apache.org/licenses/LICENSE-2.0))

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an 
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the 
specific language governing permissions and limitations under the License.

The code was Copyright 2018-2019 Game Analytics Limited and/or its affiliates. 

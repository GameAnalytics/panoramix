# ElixirDruid
[![Build Status](https://travis-ci.com/GameAnalytics/elixir_druid.svg?token=7iC72mSUZcJMSAvPBsAL&branch=master)](https://travis-ci.com/GameAnalytics/elixir_druid)

A library for sending requests to [Druid][druid], based on
[HTTPoison][httpoison].

[druid]: http://druid.io/
[httpoison]: https://github.com/edgurgel/httpoison

## Usage

Build a query like this:

```elixir
use ElixirDruid
q = from "my_datasource",
      query_type: "timeseries",
      intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      granularity: :day,
      filter: dimensions.foo == "bar",
      aggregations: [event_count: count(),
                     unique_ids: hyperUnique(:user_unique)]
```

And then send it:

```elixir
ElixirDruid.post_query :default, q
```

`:default` is a configuration profile pointing to your Druid server.
See `config/config.exs`, where you can change the profile or add new
ones.

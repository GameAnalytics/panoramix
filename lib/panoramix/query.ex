defmodule Panoramix.Query do
  @moduledoc """
  Provides functions for building Druid query requests.
  """
  defstruct [query_type: nil, data_source: nil, intervals: nil, granularity: nil,
             aggregations: nil, post_aggregations: nil, filter: nil,
             dimension: nil, dimensions: nil, metric: nil, threshold: nil, context: nil,
             to_include: nil, merge: nil, analysis_types: nil, limit_spec: nil,
             bound: nil, virtual_columns: nil, limit: nil, search_dimensions: nil,
             query: nil, sort: nil]

  # A query has type Panoramix.query.t()
  @type t :: %__MODULE__{}

  @doc """
  Use `from` macro to build Druid queries. See [Druid documentation](http://druid.io/docs/latest/querying/querying.html) to learn about
  available fields and general query object structure.

  ## Example

    ```elixir
      iex(1)> use Panoramix
      Panoramix.Query
      iex(2)> q = from "my_datasource",
      ...(2)>       query_type: "timeseries",
      ...(2)>       intervals: ["2019-03-01T00:00:00+00:00/2019-03-04T00:00:00+00:00"],
      ...(2)>       granularity: :day,
      ...(2)>       filter: dimensions.foo == "bar",
      ...(2)>        aggregations: [event_count: count(),
      ...(2)>                       unique_id_count: hyperUnique(:user_unique)]
      %Panoramix.Query{
      aggregations: [
        %{name: :event_count, type: "count"},
        %{fieldName: :user_unique, name: :unique_id_count, type: :hyperUnique}
      ],
      analysis_types: nil,
      bound: nil,
      context: %{priority: 0, timeout: 120000},
      data_source: "my_datasource",
      dimension: nil,
      dimensions: nil,
      filter: %{dimension: "foo", type: "selector", value: "bar"},
      granularity: :day,
      intervals: ["2019-03-01T00:00:00+00:00/2019-03-04T00:00:00+00:00"],
      limit: nil,
      limit_spec: nil,
      merge: nil,
      metric: nil,
      post_aggregations: nil,
      query: nil,
      query_type: "timeseries",
      search_dimensions: nil,
      sort: nil,
      threshold: nil,
      to_include: nil,
      virtual_columns: nil
      }
    ```

  Some HLL aggregation names are capitalized and therefore won't play well with the macro. For such cases
  use their aliases as a workaround:
  `hllSketchBuild`, `hllSketchMerge`, `hllSketchEstimate`, `hllSketchUnion`, `hllSketchToString`.

  The aggregation aliases will be replaced with original names when building a query.

  ## Example

    ```elixir
      iex(1)> use Panoramix
      Panoramix.Query
      iex(2)> query = from "my_datasource",
      ...(2)>       query_type: "timeseries",
      ...(2)>       intervals: ["2018-05-29T00:00:00+00:00/2018-06-05T00:00:00+00:00"],
      ...(2)>       granularity: :day,
      ...(2)>       aggregations: [event_count: count(),
      ...(2)>                     unique_ids: hllSketchMerge(:user_unique, round: true)]
      %Panoramix.Query{
        aggregations: [
          %{name: :event_count, type: "count"},
          %{
            fieldName: :user_unique,
            name: :unique_ids,
            round: true,
            type: "HLLSketchMerge"
          }
        ],
        ...
      }
    ```

  """
  @doc since: "1.0.0"
  defmacro from(source, kw) do
    # Supply default "context" parameters (timeout, priority) so that
    # we always have some to work with. If these have already been supplied
    # in kw then defaults will be overwritten.
    query_fields = [context: default_context()] ++ List.foldl(kw, [], &build_query/2)
    quote generated: true, bind_quoted: [source: source, query_fields: query_fields] do
      query =
        case source do
          %Panoramix.Query{} ->
            # Are we extending an existing query?
            source
          _ ->
            # Are we creating a new query from scratch, given some kind of datasource?
            %Panoramix.Query{data_source: Panoramix.Query.datasource(source)}
        end
      Map.merge(query, Map.new query_fields)
    end
  end

  @doc nil
  # exported only so that the `from` macro can call it.
  def datasource(datasource) when is_binary(datasource) do
    # We're using a named datasource as the source for the query
    datasource
  end
  def datasource(%{type: :query, query: nested_query} = datasource) do
    # The datasource is a nested query. Let's convert it to JSON if needed
    nested_query_json =
      case nested_query do
        %Panoramix.Query{} ->
          to_map(nested_query)
        _ ->
          # Assume it's already JSON-shaped
          nested_query
      end
    %{datasource | query: nested_query_json}
  end
  def datasource(%{type: :join, left: left, right: right} = datasource) do
    # A join between two datasources.
    # A named datasource and a recursive join can only appear on the
    # left side, but let's let Druid enforce that.
    left_datasource = datasource(left)
    right_datasource = datasource(right)
    %{datasource | left: left_datasource, right: right_datasource}
  end
  def datasource(%{type: type} = datasource) when is_atom(type) do
    # Some other type of datasource. Let's include it literally.
    datasource
  end

  defp default_context() do
    quote generated: true do
      # Let's add a timeout in the query "context", as we need to
      # tell Druid to cancel the query if it takes too long.
      # We're going to close the HTTP connection on our end, so
      # there is no point in Druid keeping processing.
      timeout = Application.get_env(:panoramix, :request_timeout, 120_000)
      # Also set the configured priority.  0 is what Druid picks if you
      # don't specify a priority, so that seems to be a sensible default.
      priority = Application.get_env(:panoramix, :query_priority, 0)
      %{timeout: timeout, priority: priority}
    end
  end

  defp build_query({field, value}, query_fields)
  when field in [:granularity, :dimension, :dimensions, :metric, :query_type,
                 :threshold, :merge, :analysis_types, :limit_spec,
                 :limit, :search_dimensions, :query, :sort] do
    # For these fields, we just include the value verbatim.
    [{field, value}] ++ query_fields
  end
  defp build_query({:bound, bound}, query_fields) do
    [bound:
     quote generated: true, bind_quoted: [bound: bound] do
       value = String.Chars.to_string bound
       unless value in ["maxTime", "minTime"] do
         raise ArgumentError, "invalid bound value '#{value}', expected 'maxTime' or 'minTime'"
       end
       value
     end
    ] ++ query_fields
  end
  defp build_query({:intervals, intervals}, query_fields) do
    [intervals: build_intervals(intervals)] ++ query_fields
  end
  defp build_query({:aggregations, aggregations}, query_fields) do
    [aggregations: build_aggregations(aggregations)] ++ query_fields
  end
  defp build_query({:post_aggregations, post_aggregations}, query_fields) do
    [post_aggregations: build_post_aggregations(post_aggregations)] ++ query_fields
  end
  defp build_query({:filter, filter}, query_fields) do
    [filter: build_filter(filter)] ++ query_fields
  end
  defp build_query({:to_include, to_include}, query_fields) do
    [to_include:
     quote do
         case unquote(to_include) do
           :all ->
             %{type: "all"}
           :none ->
             %{type: "none"}
           list when is_list(list) ->
             %{type: "list", columns: list}
         end
     end] ++ query_fields
  end
  defp build_query({:virtual_columns, virtual_columns}, query_fields) do
    [virtual_columns: build_virtual_columns(virtual_columns)] ++ query_fields
  end
  defp build_query({:context, context}, query_fields) do
    [context: build_context(context)] ++ query_fields
  end
  defp build_query({unknown, _}, _query_fields) do
    raise ArgumentError, "Unknown query field #{inspect unknown}"
  end

  defp build_intervals(intervals) do
    # mark as "generated" to avoid warnings about unreachable case
    # clauses when interval is a constant
    quote generated: true, bind_quoted: [intervals: intervals] do
      Enum.map intervals, fn
        interval_string when is_binary(interval_string) ->
          # Already a string - pass it on unchanged
          interval_string
        {from, to} ->
          Panoramix.format_time!(from) <> "/" <> Panoramix.format_time!(to)
      end
    end
  end

  defp build_aggregations(aggregations) do
    Enum.map aggregations, &build_aggregation/1
  end

  defp build_aggregation({name, {:count, _, []}}) do
    quote do: %{type: "count", name: unquote name}
  end
  defp build_aggregation({name, {:when, _, [aggregation, filter]}}) do
    # XXX: is it correct to put the name on the "inner" aggregation,
    # instead of the filtered one?
    quote generated: true, bind_quoted: [
      filter: build_filter(filter),
      aggregator: build_aggregation({name, aggregation})]
      do
      case filter do
        nil ->
          # There is no filter - just use the plain aggregator
          aggregator
        _ ->
          %{type: "filtered",
            filter: filter,
            aggregator: aggregator}
      end
    end
  end
  defp build_aggregation({name, {aggregation_type, _, [field_name]}}) do
    # e.g. hyperUnique(:user_unique)
    normalized_aggregation_type = normalize_aggregation_type_name(aggregation_type)
    quote do: %{type: unquote(normalized_aggregation_type),
        name: unquote(name),
        fieldName: unquote(field_name)}
  end
  defp build_aggregation({name, {aggregation_type, _, [field_name, keywords]}}) do
    # e.g. hyperUnique(:user_unique, round: true)
    normalized_aggregation_type = normalize_aggregation_type_name(aggregation_type)
    quote generated: true, bind_quoted: [
      aggregation_type: normalized_aggregation_type,
      name: name,
      field_name: field_name,
      keywords: keywords]
      do
      Map.merge(
        %{type: aggregation_type,
          name: name,
          fieldName: field_name},
        Map.new(keywords))
    end
  end

  # Some capitalized aggregation names need normalizing. See docs for more info.
  defp normalize_aggregation_type_name(:hllSketchBuild), do:
    "HLLSketchBuild"
  defp normalize_aggregation_type_name(:hllSketchMerge), do:
    "HLLSketchMerge"
  defp normalize_aggregation_type_name(:hllSketchEstimate), do:
    "HLLSketchEstimate"
  defp normalize_aggregation_type_name(:hllSketchEstimateWithBounds), do:
    "HLLSketchEstimateWithBounds"
  defp normalize_aggregation_type_name(:hllSketchUnion), do:
    "HLLSketchUnion"
  defp normalize_aggregation_type_name(:hllSketchToString), do:
    "HLLSketchToString"
  defp normalize_aggregation_type_name(name), do:
    name

  defp build_post_aggregations(post_aggregations) do
    Enum.map post_aggregations,
    fn {name, post_aggregation} ->
      pa = build_post_aggregation(post_aggregation)
      quote do
        Map.put(unquote(pa), :name, unquote(name))
      end
    end
  end

  defp build_post_aggregation({arith_op, _, [a, b]})
  when arith_op in [:+, :-, :*, :/] do
    pa1 = build_post_aggregation(a)
    pa2 = build_post_aggregation(b)
    quote do
      %{type: "arithmetic",
        fn: unquote(arith_op),
        fields: [unquote(pa1), unquote(pa2)]}
    end
  end
  defp build_post_aggregation({{:., _, [{:aggregations, _, _}, aggregation]}, _, _}) do
    # aggregations.foo
    quote do
      %{type: "fieldAccess",
        fieldName: unquote(aggregation)}
    end
  end
  defp build_post_aggregation({{:., _, [Access, :get]}, _, [{:aggregations, _, _}, aggregation]}) do
    # aggregations["foo"]
    quote do
      %{type: "fieldAccess",
        fieldName: unquote(aggregation)}
    end
  end
  defp build_post_aggregation(constant) when is_number(constant) do
    quote do
      %{type: "constant",
        value: unquote(constant)}
    end
  end
  defp build_post_aggregation({:expression, _, [expression]}) do
    {:%{}, [], [{:type, "expression"}, {:expression, expression}]}
  end
  defp build_post_aggregation({post_aggregator, _, [field | options]})
  when post_aggregator in [:hllSketchToString, :hllSketchEstimateWithBounds, :hllSketchEstimate] do
    field_ref = build_post_aggregation(field)
    post_aggregation_field_accessor(post_aggregator, :field, field_ref, options)
  end
  defp build_post_aggregation({:hllSketchUnion, _, [fields | options]}) do
    pa_list = for field <- fields, do: build_post_aggregation(field)
    post_aggregation_field_accessor(:hllSketchUnion, :fields, pa_list, options)
  end
  defp build_post_aggregation({post_aggregator, _, [fields]})
  when post_aggregator in [:doubleGreatest, :longGreatest, :doubleLeast, :longLeast] do
    pa_list = for field <- fields, do: build_post_aggregation(field)
    post_aggregation_field_accessor(post_aggregator, :fields, pa_list)
  end
  defp build_post_aggregation({post_aggregator, _, [field_name | options]}) do
    # This is for all post-aggregators that use a "fieldName" parameter,
    # and optionally a bunch of extra parameters.
    post_aggregation_field_accessor(post_aggregator, :fieldName, field_name, options)
  end

  def post_aggregation_field_accessor(type_name, accessor_name, accessor, options \\ []) do
    type_name = normalize_aggregation_type_name(type_name)
    options = List.first(options) || []
    {:%{}, [], [{:type, type_name}, {accessor_name, accessor} | options]}
  end

  defp build_filter({:== = operator, _, [a, b]}) do
    build_eq_filter(operator, a, b)
  end
  defp build_filter({:!= = operator, _, [a, b]}) do
    eq_filter = build_eq_filter(operator, a, b)
    {:%{}, [], [type: "not", field: eq_filter]}
  end
  defp build_filter({:and, _, [a, b]}) do
    filter_a = build_filter(a)
    filter_b = build_filter(b)
    quote generated: true do
      case {unquote(filter_a), unquote(filter_b)} do
        {nil, nil} ->
          # No filter AND no filter: that's "no filter"
          nil
        {nil, filter} ->
          # No filter AND filter: just one filter
          filter
        {filter, nil} ->
          # Likewise
          filter
        # If either or both filter is an AND already, merge them together
        {filter_a_unquoted, filter_b_unquoted} ->
          # Need to handle both atom and string keys
          a_is_and = unquote(atom_or_string_value(quote(do: filter_a_unquoted), :type)) == "and"
          b_is_and = unquote(atom_or_string_value(quote(do: filter_b_unquoted), :type)) == "and"
          filter_a_fields = unquote(atom_or_string_value(quote(do: filter_a_unquoted), :fields))
          filter_b_fields = unquote(atom_or_string_value(quote(do: filter_b_unquoted), :fields))
          case {a_is_and, b_is_and} do
            {true, true} ->
              %{type: "and", fields: filter_a_fields ++ filter_b_fields}
            {true, false} ->
              %{type: "and", fields: filter_a_fields ++ [filter_b_unquoted]}
            {false, true} ->
              %{type: "and", fields: [filter_a_unquoted] ++ filter_b_fields}
            {false, false} ->
              %{type: "and", fields: [filter_a_unquoted, filter_b_unquoted]}
          end
      end
    end
  end
  defp build_filter({:or, _, [a, b]}) do
    filter_a = build_filter(a)
    filter_b = build_filter(b)
    quote generated: true do
      # It's not meaningful to use 'or' with the empty filter,
      # since the empty filter already allows anything.
      case {unquote(filter_a), unquote(filter_b)} do
        {nil, _} ->
          raise "left operand to 'or' must not be nil"
        {_, nil} ->
          raise "right operand to 'or' must not be nil"
        {filter_a_unquoted, filter_b_unquoted} ->
          %{type: "or", fields: [filter_a_unquoted, filter_b_unquoted]}
      end
    end
  end
  defp build_filter({:not, _, [a]}) do
    filter = build_filter(a)
    quote generated: true do
      # It's not meaningful to use 'not' with the empty filter,
      # since "not everything" would allow "nothing".
      case unquote(filter) do
        nil ->
          raise "operand to 'not' must not be nil"
        filter_unquoted ->
          %{type: "not", field: filter_unquoted}
      end
    end
  end
  # Let's handle the 'in' operator.  First, let's handle
  # dimensions.foo in intervals([a, b])
  # (where 'foo' will usually be '__time', a special dimension for
  # the event timestamp)
  defp build_filter({:in, _, [a, {:intervals, _, [intervals]}]}) do
    dimension = dimension_or_extraction_fn(a)
    unless dimension do
      raise "left operand of 'in' must be a dimension"
    end
    {:%{}, [], [
        type: "interval",
        intervals: build_intervals(intervals)] ++
      # allow extraction function
      Map.to_list(dimension)}
  end
  # Now handle
  # dimensions.foo in ["123", "456"]
  defp build_filter({:in, _, [a, values]}) do
    dimension = dimension_or_extraction_fn(a)
    unless dimension do
      raise "left operand of 'in' must be a dimension"
    end
    {:%{}, [], [
        type: "in",
        values: values] ++
      # allow extraction function
      Map.to_list(dimension)}
  end
  defp build_filter({lt1, _, [{lt2, _, [a, b]}, c]})
    when lt1 in [:<, :<=] and lt2 in [:<, :<=] do
    # 1 < dimensions.foo < 10, or
    # 1 <= dimensions.foo <= 10
    #
    # Note that operator precedence and associativity gives:
    # ((a < b) < c)
    # so lt2 is actually the one that appears first in the
    # source code.
    lower_strict = (lt2 == :<)
    upper_strict = (lt1 == :<)
    dimension = dimension_or_extraction_fn(b)
    unless dimension do
      raise "middle operand in bound filter must be a dimension"
    end
    base = {:%{}, [], [type: "bound", lowerStrict: lower_strict, upperStrict: upper_strict] ++
      # allow extraction function
      Map.to_list(dimension)}
    # Need 'generated: true' here to avoid compiler warnings for
    # our case expression in case a and c are literal constants.
    quote generated: true do
      # Need to convert bounds to strings, and select sorting order.
      # Let's go for "numeric" when both are numbers, "lexicographic"
      # when both are strings, and crash otherwise.
      # TODO: do we need "alphanumeric" and "strlen"?
      {lower, upper, ordering} =
        case {unquote(a), unquote(c)} do
          {l, u} when is_integer(l) and is_integer(u) ->
            {Integer.to_string(l), Integer.to_string(u), "numeric"}
          {l, u} when is_float(l) and is_float(u) ->
            {Float.to_string(l), Float.to_string(u), "numeric"}
          {l, u} when is_binary(l) and is_binary(u) ->
            {l, u, "lexicographic"}
        end
      Map.merge(unquote(base),
        %{lower: lower,
          upper: upper,
          ordering: ordering})
    end
  end
  defp build_filter({:expression, _, [expression]}) do
    # A math expression, as described in http://druid.io/docs/0.12.1/misc/math-expr
    # We're expecting a string that we're passing on to Druid
    quote bind_quoted: [expression: expression] do
      %{type: "expression",
        expression: expression}
    end
  end
  defp build_filter({:^, _, [expression]}) do
    # We're recycling the ^ operator to incorporate an already created
    # filter into a filter expression.
    quote generated: true, bind_quoted: [expression: expression] do
      case expression do
        %{type: _} = filter ->
          # Looks like a filter!
          filter
        %{"type" => _} = filter ->
          # Same, but the keys are strings, not atoms
          filter
        nil ->
          # nil is a valid filter as well
          nil
      end
    end
  end

  defp build_eq_filter(operator, a, b) do
    dimension_a = dimension_or_extraction_fn(a)
    dimension_b = dimension_or_extraction_fn(b)
    case {dimension_a, dimension_b} do
      {nil, _} ->
        raise "left operand of #{operator} must be a dimension"
      {_, nil} ->
        # Compare a dimension to a value
        {:%{}, [], [
            type: "selector",
            value: b] ++
          # dimension_a is either just a dimension, or a dimension
          # plus an extraction function
          Map.to_list(dimension_a)}
      {_, _} ->
        # Compare two dimensions
        dimension_spec_a = to_dimension_spec(dimension_a)
        dimension_spec_b = to_dimension_spec(dimension_b)
        quote do: %{type: "columnComparison",
                    dimensions: [unquote(dimension_spec_a),
                                 unquote(dimension_spec_b)]}
    end
  end

  defp atom_or_string_value(map, key_atom) do
    # Given a macro fragment that evaluates to a map, and an atom,
    # return a macro fragment that returns the value of that atom
    # in the map, or the value of the corresponding string in the map,
    # or nil if neither is present in the map.
    var = Macro.unique_var(:x, __MODULE__)
    key_string = Atom.to_string(key_atom)
    {:case, [generated: true], [
        map,
        [do: [
          {:->, [generated: true], [[{:%{}, [], [{key_atom, var}]}], var]},
          {:->, [generated: true], [[{:%{}, [], [{key_string, var}]}], var]},
          {:->, [generated: true], [[{:%{}, [], []}], nil]}]]]}
  end

  # TODO: handle more extraction functions
  defp dimension_or_extraction_fn({{:., _, [{:dimensions, _, _}, dimension]}, _, _}) do
    # dimensions.foo
    %{dimension: Atom.to_string(dimension)}
  end
  defp dimension_or_extraction_fn({{:., _, [Access, :get]}, _, [{:dimensions, _, _}, dimension]}) do
    # dimensions["foo"]
    %{dimension: dimension}
  end
  defp dimension_or_extraction_fn({:lookup, _, args}) do
    case args do
      [lookup_name | maybe_opts] ->
        opts = case maybe_opts do
                 [] -> []
                 [opts] -> opts
               end
        %{extractionFn: {:%{}, [],
                         [{"type", "registeredLookup"},
                          {"lookup", lookup_name}] ++ opts}}
      _ ->
        raise ArgumentError, "Expected lookup name as argument to lookup"
    end
  end
  defp dimension_or_extraction_fn({:|>, _, [left, right]}) do
    left = dimension_or_extraction_fn(left)
    right = dimension_or_extraction_fn(right)
    case {left, right} do
      {%{dimension: dimension, extractionFn: left_extraction_fn}, %{extractionFn: right_extraction_fn}} ->
        # There are extraction functions on both sides of the operator
        # - let's combine them into a cascade extraction function.
        %{dimension: dimension,
          extractionFn: {:%{}, [],
                         [{"type", "cascade"},
                          {"extractionFns", [left_extraction_fn, right_extraction_fn]}]}}
      {%{dimension: dimension}, %{extractionFn: extraction_fn}} ->
        %{dimension: dimension, extractionFn: extraction_fn}
    end
  end
  defp dimension_or_extraction_fn(_) do
    nil
  end

  defp to_dimension_spec(%{dimension: dimension, extractionFn: extraction_fn}) do
    # Do we need to set outputName here?
    {:%{}, [], [type: "extraction",
                dimension: dimension,
                extractionFn: extraction_fn]}
  end
  defp to_dimension_spec(%{dimension: dimension}) do
    dimension
  end

  defp build_virtual_columns(virtual_columns) do
    Enum.map virtual_columns, &build_virtual_column/1
  end

  defp build_virtual_column({name, {:expression, _, [expression, output_type]}}) do
    quote generated: true, bind_quoted: [
      name: name,
      expression: expression,
      output_type: output_type
    ] do
      output_type = String.upcase(String.Chars.to_string(output_type))
      unless output_type in ["LONG", "FLOAT", "DOUBLE", "STRING"] do
        raise ArgumentError, "Unexpected output type #{output_type}, expected one of :long, :float, :double, :string"
      end
      %{"type" => "expression",
        "name" => name,
        "outputType" => output_type,
        "expression" => expression}
    end
  end
  defp build_virtual_column({_name, {:expression, _, args}}) do
    raise ArgumentError, "Expected 2 arguments to 'expression' in virtual column, expression and output type; " <>
      "got #{length args}"
  end

  defp build_context(context) do
    quote generated: true, bind_quoted: [context: context, default_context: default_context()] do
      Map.merge(default_context, context)
    end
  end

  @doc """
  Convert a Panoramix.Query struct into a map ready to be converted to JSON.
  """
  def to_map(%Panoramix.Query{} = query) do
    unless query.query_type do
      raise "query type not specified"
    end
    [queryType: query.query_type,
     dataSource: query.data_source,
     intervals: query.intervals,
     granularity: query.granularity,
     aggregations: query.aggregations,
     postAggregations: query.post_aggregations,
     filter: query.filter,
     dimension: query.dimension,
     dimensions: query.dimensions,
     metric: query.metric,
     threshold: query.threshold,
     context: query.context,
     toInclude: query.to_include,
     merge: query.merge,
     analysisTypes: query.analysis_types,
     limitSpec: query.limit_spec,
     bound: query.bound,
     virtualColumns: query.virtual_columns,
     limit: query.limit,
     searchDimensions: query.search_dimensions,
     query: query.query,
     sort: query.sort,
    ]
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.into(%{})
  end

  @doc """
  Convert a Panoramix.Query struct into its JSON representation.
  """
  def to_json(query) do
    query
    |> to_map()
    |> Jason.encode!
  end
end

defmodule ElixirDruid.Query do
  defstruct [query_type: nil, data_source: nil, intervals: [], granularity: nil,
	     aggregations: [], post_aggregations: nil, filter: nil,
             dimension: nil, metric: nil, threshold: nil, context: nil]

  defmacro build(query_type, data_source, kw \\ []) do
    query_fields = [
      query_type: query_type,
      data_source: data_source
    ]
    query_fields = List.foldl(kw, query_fields, &build_query/2)
    quote do
      ElixirDruid.Query.__struct__(unquote(query_fields))
    end
  end

  defmacro set(query, kw) do
    query_fields = List.foldl(kw, [], &build_query/2)
    quote do
      Map.merge(unquote(query), Map.new unquote(query_fields))
    end
  end

  defp build_query({:intervals, intervals}, query_fields) do
    # TODO: process intervals somehow?
    [intervals: intervals] ++ query_fields
  end
  defp build_query({:granularity, granularity}, query_fields) do
    [granularity: granularity] ++ query_fields
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
  defp build_query({:dimension, dimension}, query_fields) do
    [dimension: dimension] ++ query_fields
  end
  defp build_query({:metric, metric}, query_fields) do
    [metric: metric] ++ query_fields
  end
  defp build_query({:threshold, threshold}, query_fields) do
    [threshold: threshold] ++ query_fields
  end
  defp build_query({:context, context}, query_fields) do
    [context: context] ++ query_fields
  end

  defp build_aggregations(aggregations) do
    Enum.map aggregations, &build_aggregation/1
  end

  defp build_aggregation({name, {:count, _, []}}) do
    quote do: %{type: "count", name: unquote name}
  end
  defp build_aggregation({name, {aggregation_type, _, [field_name]}}) do
    quote do: %{type: unquote(aggregation_type),
		name: unquote(name),
		fieldName: unquote(field_name)}
  end
  defp build_aggregation({name, {:when, _, [aggregation, filter]}}) do
    # XXX: is it correct to put the name on the "inner" aggregation,
    # instead of the filtered one?
    quote do
      %{type: "filtered",
        filter: unquote(build_filter(filter)),
        aggregator: unquote(build_aggregation({name, aggregation}))}
    end
  end

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
  defp build_post_aggregation({:hyperUniqueCardinality, _, [field_name]}) do
    quote do
      %{type: "hyperUniqueCardinality",
        fieldName: unquote(field_name)}
    end
  end
  defp build_post_aggregation({post_aggregator, _, fields = [_|_]})
  when post_aggregator in [:doubleGreatest, :longGreatest, :doubleLeast, :longLeast] do
    quote do
      %{type: unquote(post_aggregator),
        fields: unquote(fields)}
    end
  end

  defp build_filter({:==, _, [a, b]}) do
    dimension_a = maybe_build_dimension(a)
    dimension_b = maybe_build_dimension(b)
    case {dimension_a, dimension_b} do
      {nil, _} ->
	raise "left operand of == must be a dimension"
      {_, nil} ->
	# Compare a dimension to a value
	quote do: %{type: "selector",
    		    dimension: unquote(dimension_a),
    		    value: unquote(b)}
      {_, _} ->
	# Compare two dimensions
	quote do: %{type: "columnComparison",
    		    dimensions: [unquote(dimension_a),
				 unquote(dimension_b)]}
    end
  end
  defp build_filter({:and, _, [a, b]}) do
    filter_a = build_filter(a)
    filter_b = build_filter(b)
    quote do: %{type: "and", fields: [unquote(filter_a), unquote(filter_b)]}
  end
  defp build_filter({:or, _, [a, b]}) do
    filter_a = build_filter(a)
    filter_b = build_filter(b)
    quote do: %{type: "or", fields: [unquote(filter_a), unquote(filter_b)]}
  end
  defp build_filter({:not, _, [a]}) do
    filter = build_filter(a)
    quote do: %{type: "not", field: unquote(filter)}
  end
  defp build_filter({:in, _, [a, values]}) do
    dimension = maybe_build_dimension(a)
    unless dimension do
      raise "left operand of 'in' must be a dimension"
    end
    quote do: %{type: "in", dimension: unquote(dimension), values: unquote(values)}
  end
  defp build_filter(
    {lt1, _, [{lt2, _, [a, b]}, c]})
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
    dimension = maybe_build_dimension(b)
    unless dimension do
      raise "middle operand in bound filter must be a dimension"
    end
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
      %{type: "bound",
	dimension: unquote(dimension),
	lower: lower,
	upper: upper,
	lowerStrict: unquote(lower_strict),
	upperStrict: unquote(upper_strict),
	ordering: ordering}
    end
  end
  defp build_filter(expression) do
    # Anything else - it's probably a map coming from an existing
    # filter.  Let's match on it at run time.
    quote do
      case unquote(expression) do
	%{type: _} = filter ->
	  # Looks like a filter!
	  filter
      end
    end
  end

  # TODO: handle dimension specs + extraction functions, not just "plain" dimensions
  defp maybe_build_dimension({{:., _, [{:dimensions, _, _}, dimension]}, _, _}) do
    # dimensions.foo
    Atom.to_string dimension
  end
  defp maybe_build_dimension({{:., _, [Access, :get]}, _, [{:dimensions, _, _}, dimension]}) do
    # dimensions["foo"]
    dimension
  end
  defp maybe_build_dimension(_) do
    nil
  end

  def to_json(query) do
    [queryType: query.query_type,
     dataSource: query.data_source,
     intervals: query.intervals,
     granularity: query.granularity,
     aggregations: query.aggregations,
     postAggregations: query.post_aggregations,
     filter: query.filter,
     dimension: query.dimension,
     metric: query.metric,
     threshold: query.threshold,
     context: query.context,
    ]
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.into(%{})
    |> Jason.encode!
  end
end

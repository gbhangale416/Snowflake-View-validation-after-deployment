import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # Retrieve input dictionary
    params = context.get_input() or {}

    # Extract the function name dynamically from input (defaults to multi-thread if omitted)
    activity_name = params.get("function_name", "snowflake_view_validator_multi_thread")

    # Call the specified activity function
    result = yield context.call_activity(activity_name, params)

    return result


main = df.Orchestrator.create(orchestrator_function)

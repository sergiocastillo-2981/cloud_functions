import functions_framework

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_args = request.args
    
    action = ""    
    instance=""

    for arg_name,arg_value in request_args.items():
        print(f"arg name: {arg_name}, arg value :{arg_value}")

        match arg_name:
            case "instance":  
                instance=arg_value

    return f'Hello Dear {instance}!'

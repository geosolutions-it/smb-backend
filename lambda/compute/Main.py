print('Loading function')

def lambda_handler(event, context):
    print("Hello")
    return { 
        'message' : 'Hello'
    }

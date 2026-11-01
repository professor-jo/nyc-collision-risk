from ingestion.ingest_collisions import main

def lambda_handler(event, context):
    '''
    Entry point called by AWS Lambda.

    event: input supplied when Lambda is invoked
    context: information about the Lambda execution environment
    '''

    # Run our existing ingestion pipeline.
    main()

    # Return a simple success response.
    return {
        "statusCode": 200,
        "message": "Collision ingestion completed"
    }

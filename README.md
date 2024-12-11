# OCR SDS Extraction Lambda Function

This AWS Lambda function processes Safety Data Sheet (SDS) documents by extracting chemical information and their corresponding CAS numbers using OpenAI's GPT model. The function analyzes both structured table data and query results to compile a comprehensive list of chemicals.

## Prerequisites

- Python 3.13
- AWS Lambda access
- OpenAI API key
- Appropriate IAM roles and permissions

## Installation

### Local Development Setup

1. Create a new directory for your project:
```bash
mkdir sds_extraction
cd sds_extraction
```

2. Create a requirements.txt file:
```bash
echo "openai" > requirements.txt
```

3. Install dependencies with the correct platform settings:
```bash
pip install --platform manylinux2014_x86_64 --target=package --implementation cp --python-version 3.13 --only-binary=:all: --upgrade -r requirements.txt
```

### Creating the Lambda Deployment Package

1. Copy your lambda_function.py into the package directory:
```bash
cp lambda_function.py package/
```

2. Create the deployment ZIP:
```bash
cd package
zip -r ../deployment-package.zip .
cd ..
```

### Updating the Lambda Function

1. Via AWS Console:
   - Open AWS Lambda Console
   - Navigate to your function
   - Click on "Upload from" dropdown
   - Choose ".zip file"
   - Upload deployment-package.zip
   - Click "Save"


## Configuration

### Environment Variables

Set up the following environment variables in your Lambda function:

- `OPENAI_API_KEY`: Your OpenAI API key

## Function URL

The function is accessible via the following URL:
```
https://ceudnjpezzgutjocg27jsfihtu0soyic.lambda-url.us-east-2.on.aws/
```

## Function Input

The function expects a JSON event with the following structure:
```json
{
    "bucket": "textract-console-us-east-2-8a84b6c0-d3c2-420d-8d03-9d742797b315",
    "document": "path_to_your_pdf.pdf"
}
```

## Function Output

The function returns a JSON object with the following structure:
```json
{
    "chemicals": [
        {    
            "name": "chemical name",
            "cas_number": "CAS number or null",
            "weight": {
                "min": "X",
                "max": "Y"
              }
        }
    ]
}
```
In my testing, the longest document took ~6 minutes to finish processing, most take 2-3 minutes.

## Error Handling

The function includes comprehensive error handling and logging. All errors are:
- Logged to CloudWatch
- Returned in the response with appropriate HTTP status codes
- Formatted consistently with the expected output structure

## Updating the Function

To update the Lambda function after making changes:

1. Make your code changes to lambda_function.py
2. Clean up any old files:
```bash
rm -rf package/
rm deployment-package.zip
```

3. Reinstall dependencies:
```bash
pip install --platform manylinux2014_x86_64 --target=package --implementation cp --python-version 3.13 --only-binary=:all: --upgrade -r requirements.txt
```

4. Copy updated lambda function:
```bash
cp lambda_function.py package/
```

5. Create new deployment package:
```bash
cd package
zip -r ../deployment-package.zip .
cd ..
```

6. Upload to AWS Lambda using either the console or CLI method described above

## Troubleshooting

Common issues and solutions:

1. Import errors:
   - Ensure all dependencies are properly installed in the package directory
   - Verify the correct Python version is selected in Lambda
   - Check the deployment package structure

2. Timeout errors:
   - Increase the Lambda function timeout in configuration
   - Consider optimizing the code if timeouts persist


## Support

For issues and questions:
1. Check CloudWatch logs for detailed error messages
2. Review the Lambda function configuration
3. Verify all environment variables are set correctly (You need an OpenAI API key)

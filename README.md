# lambda-sqlite3

Create a Lambda Layer and Lambda Function to use S3 bucket and state function

## Project Creation and Installation Steps

1. **Clone the repository:**

    ```sh
    git clone https://github.com/meera/lambda-sqlite3.git
    cd lambda-sqlite3
    ```

2. **Install dependencies:**

    ```sh
    npm install
    ```

    Create lambda-function.zip

    ```sh
    npm run build-aws-lambda-fn
    ```

    Create lambda-layer.zip

    ```sh
    npm run build-aws-lambda-layer
    ```

## AWS CLI (Optional)

1. **Install dependencies:**

    ```sh
    npm install
    ```

2. **Set up AWS credentials:**
    Ensure your AWS credentials are configured properly. You can use the AWS CLI to configure your credentials:

    ```sh
    aws configure
    ```

3. **Deploy the Lambda function:**

    ```sh
    serverless deploy
    ```

4. **Verify the deployment:**
    Check the AWS Lambda console to ensure the function is deployed successfully.

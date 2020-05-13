$env:AWS_PROFILE="kranio"

cd infra
serverless deploy --stage dev
cd..

cd utils-shared-lib
serverless deploy --stage dev
cd..

cd analytics
serverless deploy --stage dev
cd..

cd orchestration
serverless deploy --stage dev
cd..

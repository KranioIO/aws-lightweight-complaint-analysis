$env:AWS_PROFILE="kranio"

cd orchestration
serverless remove --stage dev
cd..

cd analytics
serverless remove --stage dev
cd..

cd utils-shared-lib
serverless remove --stage dev
cd..

cd infra
serverless remove --stage dev
cd..

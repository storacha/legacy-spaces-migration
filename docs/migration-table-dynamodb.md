### Staging

```sh
aws dynamodb create-table \
  --table-name staging-migration-progress \
  --attribute-definitions \
    AttributeName=customer,AttributeType=S \
    AttributeName=space,AttributeType=S \
    AttributeName=status,AttributeType=S \
    AttributeName=updatedAt,AttributeType=S \
  --key-schema \
    AttributeName=customer,KeyType=HASH \
    AttributeName=space,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --global-secondary-indexes \
    "[{
      \"IndexName\": \"status-index\",
      \"KeySchema\": [
        {\"AttributeName\":\"status\",\"KeyType\":\"HASH\"},
        {\"AttributeName\":\"updatedAt\",\"KeyType\":\"RANGE\"}
      ],
      \"Projection\": {\"ProjectionType\":\"ALL\"}
    }]" \
  --region us-east-2
```


### Production

```sh
aws dynamodb create-table \
  --table-name prod-migration-progress \
  --attribute-definitions \
    AttributeName=customer,AttributeType=S \
    AttributeName=space,AttributeType=S \
    AttributeName=status,AttributeType=S \
    AttributeName=updatedAt,AttributeType=S \
  --key-schema \
    AttributeName=customer,KeyType=HASH \
    AttributeName=space,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --global-secondary-indexes \
    "[{
      \"IndexName\": \"status-index\",
      \"KeySchema\": [
        {\"AttributeName\":\"status\",\"KeyType\":\"HASH\"},
        {\"AttributeName\":\"updatedAt\",\"KeyType\":\"RANGE\"}
      ],
      \"Projection\": {\"ProjectionType\":\"ALL\"}
    }]" \
  --region us-west-2
```
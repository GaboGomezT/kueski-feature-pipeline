build:
	docker build -t pyspark .

run:
	docker run -v ~/.aws/credentials:/root/.aws/credentials --env-file ./.env -it pyspark
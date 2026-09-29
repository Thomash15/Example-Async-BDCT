docker-logs:
	@docker-compose -f kafka-cluster.yml logs -f

docker-rm:
	@docker-compose -f kafka-cluster.yml rm -vfs

docker-stop:
	@docker-compose -f kafka-cluster.yml stop

docker:
	@docker-compose -f kafka-cluster.yml up -d --no-recreate

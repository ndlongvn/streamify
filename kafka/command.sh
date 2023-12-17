docker network create project-network
docker-compose -f docker-compose_all.yml -f docker-compose_broker1.yml -f docker-compose_broker2 up -d

# down: docker-compose -f docker-compose_all.yml -f docker-compose_broker1.yml -f docker-compose_broker2.yml down
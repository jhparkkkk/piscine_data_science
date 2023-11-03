all:
	@docker-compose -f docker-compose.yml up --build

exec: 
	@docker exec -it postgres_container bash
down:
	@docker-compose -f docker-compose.yml down -v

re:	down
	@docker-compose -f docker-compose.yml up -d --build

clean: down
	@docker system prune -a

fclean:
	@docker compose down
	@docker system prune --all --force --volumes
	@docker network prune --force
	@docker volume prune --force

.PHONY	: all build down re clean fclean
docker_build:
	docker build -t centuri/centuri-omero-server .
	docker push centuri/centuri-omero-server

docker_test:
	cd tests/omero && docker-compose pull \
				   && docker-compose up -d \
				   && docker-compose exec omeroserver \
					  /opt/omero/server/impomero/tests/omero/add_users.sh
				   && docker-compose exec omeroserver \
					  /opt/omero/server/impomero/tests/omero/run_impomero.sh

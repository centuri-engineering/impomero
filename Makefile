docker_build:
	docker build -t centuri/centuri-omero-server .
	docker push centuri/centuri-omero-server

docker_test:
	cd tests/omero && docker-compose pull \
				   && docker-compose up -d
	sleep 60
	cd tests/omero && docker-compose exec omeroserver \
					  /opt/omero/server/impomero/tests/omero/add_users.sh \
				   && docker-compose exec omeroserver \
					  /opt/omero/server/impomero/tests/omero/run_impomero.sh

docker_down:
	cd tests/omero && docker-compose down

docker-clean:
	docker volume rm test_omero_backup
	docker volume rm test_omero_data
	docker volume rm test_omero_db_omero
	docker volume rm test_omero_omero

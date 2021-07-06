FROM openmicroscopy/omero-server:latest

USER root
# RUN yum install -y cifs-utils


USER omero-server
WORKDIR /opt/omero/server/
RUN wget https://github.com/centuri-engineering/impomero/archive/refs/heads/main.zip
RUN unzip main.zip
WORKDIR impomero-main

USER root
RUN /opt/omero/server/venv3/bin/python -m pip install -r requirements.txt
RUN /opt/omero/server/venv3/bin/python -m pip install -e .

USER omero-server

WORKDIR /opt/omero
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

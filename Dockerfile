FROM openmicroscopy/omero-server:latest

USER root
# RUN yum install -y cifs-utils


USER omero-server
WORKDIR /opt/omero/server/
COPY . /opt/omero/server/impomero/

WORKDIR impomero

USER root
RUN /opt/omero/server/venv3/bin/python -m pip install -r requirements.txt
RUN /opt/omero/server/venv3/bin/python -m pip install -e .

USER omero-server

WORKDIR /opt/omero
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

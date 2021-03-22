FROM centos/python-38-centos7:latest
USER root

ENV PYTHONUNBUFFERED True

WORKDIR /vuanem_ns
COPY . ./

RUN mkdir -p /opt/netsuite/odbcclient \
    && unzip NetSuiteODBCDrivers_Linux64bit.zip -d /opt/netsuite/odbcclient
RUN sed -i 's/NetSuite=/NetSuiteML=/g' /opt/netsuite/odbcclient/odbc64.ini \
    && sed -i 's/\[NetSuite\]/\[NetSuiteML\]/g' /opt/netsuite/odbcclient/odbc64.ini

RUN yum -y install epel-release \
    && yum -y install gcc-c++ python3-devel unixODBC-devel \
    && yum clean all
    
RUN pip install pipenv \
    && pipenv install --system --skip-lock

ENV LD_LIBRARY_PATH=/opt/netsuite/odbcclient/lib64${LD_LIBRARY_PATH:+":"}${LD_LIBRARY_PATH:-""}
ENV OASDK_ODBC_HOME=/opt/netsuite/odbcclient/lib64
ENV ODBCINI=/opt/netsuite/odbcclient/odbc64.ini

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

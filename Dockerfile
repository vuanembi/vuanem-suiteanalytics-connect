FROM centos/python-38-centos7:latest
USER root

COPY . .
SHELL ["/bin/bash", "-c"]

RUN wget "https://4975572.app.netsuite.com/app/external/odbc/odbcDriverZipDownload.nl?type=BIT_64&os=LINUX" > NetSuiteODBCDrivers_Linux64bit.zip \
    && mkdir -p /opt/netsuite/odbcclient \
    && unzip NetSuiteODBCDrivers_Linux64bit.zip -d /opt/netsuite/odbcclient
RUN sed -i 's/NetSuite=/NetSuiteML=/g' /opt/netsuite/odbcclient/odbc64.ini \
    && sed -i 's/\[NetSuite\]/\[NetSuiteML\]/g' /opt/netsuite/odbcclient/odbc64.ini

RUN yum -y install epel-release \
    && yum -y install gcc-c++ python3-devel unixODBC-devel \
    && yum clean all
    
RUN pip install pipenv \
    && pipenv install --system --skip-lock \
    && pipenv install --dev --system --skip-lock

WORKDIR /etl

ENV LD_LIBRARY_PATH=/opt/netsuite/odbcclient/lib64${LD_LIBRARY_PATH:+":"}${LD_LIBRARY_PATH:-""}
ENV OASDK_ODBC_HOME=/opt/netsuite/odbcclient/lib64
ENV ODBCINI=/opt/netsuite/odbcclient/odbc64.ini

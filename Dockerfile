FROM centos/python-38-centos7:latest
USER root
ENV PYTHONUNBUFFERED True

RUN yum update -y \
    && yum install -y java-1.8.0-openjdk \
    && yum clean all

WORKDIR /vuanem_ns
COPY . ./

RUN pip install pipenv \
    && pipenv install --system --skip-lock

EXPOSE 8080

ENV PORT=8080

CMD exec functions-framework --target=main

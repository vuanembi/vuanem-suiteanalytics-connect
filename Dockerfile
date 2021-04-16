FROM adoptopenjdk/openjdk8:centos-slim
COPY --from=python:3.8-slim / /
USER root
ENV PYTHONUNBUFFERED True

WORKDIR /vuanem_ns
COPY . ./

RUN pip install pipenv --no-cache-dir \
    && pipenv install --system --deploy

EXPOSE 8080

ENV PORT=8080

CMD exec functions-framework --target=main

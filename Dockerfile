FROM ubuntu:24.04

ENV GUNICORN_WORKERS="1"
# For a 2 core machine, 5 workers is a good starting point
# For 8 core machine, 17 workers is a good starting point

ENV BIND_HOST="0.0.0.0:8000"
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y make

# Remove unnecessary directories & files
RUN rm -rf docs
RUN rm -rf tests

# Set working directory and copy the project code
WORKDIR /app
COPY . /app

ENV PYTHONPATH="/app"

# Install project dependencies
RUN uv venv --python 3.13
# Pin to the latest version of Python
RUN make dependencies

RUN uv cache prune --ci

EXPOSE 8000

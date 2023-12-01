# Build wheels for all dependencies
FROM python:3.7-slim-bullseye as wheel-builder
WORKDIR /project
RUN echo "" > README.md
COPY setup.py ./
RUN pip wheel --only-binary ':all:' --wheel-dir=/wheels .

# Start a new build stage, copying over built wheels
FROM python:3.7-slim-bullseye
COPY --from=wheel-builder /wheels /wheels

# Install the MatchEngine package
WORKDIR /project
COPY setup.py README.md .
COPY matchengine ./matchengine
RUN pip install --no-cache-dir --compile --no-index --find-links=/wheels .

# Set a default SECRETS_JSON value and entrypoint
ENV SECRETS_JSON /secrets.json
ENTRYPOINT ["matchengine"]

# One Dockerfile for every Python microservice; the service is chosen with --build-arg SERVICE=<dir>.
# Build from the repository root:
#   docker build -f services/Dockerfile --build-arg SERVICE=building-registry -t building-registry .
FROM python:3.12-slim

ARG SERVICE
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app

COPY services/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# shared library + generated gRPC stubs for the 2PC protocol
COPY proto /app/proto
COPY services/common /app/common
RUN python -m grpc_tools.protoc -I /app/proto --python_out=/app/common --grpc_python_out=/app/common \
        /app/proto/sbproto/twopc.proto \
 && pip install --no-cache-dir /app/common

COPY services/${SERVICE} /app/service
WORKDIR /app/service
RUN test -f app.py && useradd --uid 10001 --no-create-home appuser
USER 10001

EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT} --no-access-log --timeout-graceful-shutdown 10"]

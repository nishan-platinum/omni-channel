# Conformance suite runner: black-box, talks to the gateway only over HTTP/WebSocket (+ Docker
# for the fault tests). Built and run by conformance/run.sh.
FROM python:3.12-slim
WORKDIR /suite
COPY requirements.txt .
RUN pip install --no-cache-dir -q -r requirements.txt
COPY . .
ENTRYPOINT ["pytest", "-p", "no:cacheprovider", "-rA"]

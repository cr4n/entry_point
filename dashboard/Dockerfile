FROM python:3.8-slim

WORKDIR /app

COPY create_dashboard.py .
COPY setup.sh .

RUN apt-get update && apt-get install -y curl jq \
    && pip install requests \
    && chmod +x setup.sh

CMD ["./setup.sh"]
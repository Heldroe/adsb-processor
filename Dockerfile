FROM python:3

LABEL org.opencontainers.image.source=https://github.com/Heldroe/adsb-processor

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

ENTRYPOINT ["python", "-m", "processor"]
CMD []

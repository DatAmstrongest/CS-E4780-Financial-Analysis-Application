FROM python:3.12-slim
RUN pip install --no-cache-dir docker==7.1.0 prometheus-client==0.21.0
COPY exporter.py /exporter.py
ENV PYTHONUNBUFFERED=1
EXPOSE 9417
CMD ["python", "/exporter.py"]

FROM python:3.12-slim

WORKDIR /app/dormpool

# The community platform runs the Master web service.  GPU workers are
# separate processes and should be started on the machines that hold the
# model checkpoint.
ENV PYTHONPATH=/app \
    DORMPOOL_DATA=/app/data \
    SHARD_SIZE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /app/dormpool/requirements.txt
RUN pip install --no-cache-dir -r /app/dormpool/requirements.txt

COPY . /app/dormpool

EXPOSE 3000
CMD ["python", "-m", "uvicorn", "src.master.main:app", "--host", "0.0.0.0", "--port", "3000"]

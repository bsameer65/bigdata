FROM python:3.8-slim

# Install dependencies from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory
WORKDIR /app

#COPY D:\TU Hamburg\Semester 2\Big data\project\radiation_data\measurements.csv .
# Copy the producer.py script
COPY producer.py .

# Set the default command to run the producer script
CMD ["python", "-u", "producer.py"]


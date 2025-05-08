FROM flink:1.17-scala_2.12-java11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip
    
# Set Python alias (because Flink image may not have 'python' shortcut)
RUN ln -s /usr/bin/python3 /usr/bin/python
    
# Set working directory
WORKDIR /app
    
# Copy your app files (optional if you mount volume)
# COPY . /app
    
# Install required Python packages (optional)
RUN pip install apache-flink kafka-python



    



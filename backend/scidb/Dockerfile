# Use Ubuntu 20.04 as base
FROM ubuntu:20.04

# Avoid interactive prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && \
    apt-get install -y wget curl gnupg2 build-essential lsb-release python3 python3-pip && \
    apt-get clean

# Download and install SciDB Community Edition 19.11
RUN wget https://downloads.paradigm4.com/scidb-ce-19.11.4/scidb-19.11.4-amd64.deb && \
    dpkg -i scidb-19.11.4-amd64.deb || apt-get install -f -y

# Copy start script
COPY start-scidb.sh /start-scidb.sh
RUN chmod +x /start-scidb.sh

# Mount folder for CSV uploads
VOLUME ["/data"]

# Expose SciDB client port
EXPOSE 8083

# Run the start script
CMD ["/start-scidb.sh"]

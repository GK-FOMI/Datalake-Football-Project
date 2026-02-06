FROM apache/airflow:2.9.2

USER root

# Installer Java 17
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Configuration Java multi-architecture
RUN if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then \
        ln -sf /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/java-17-openjdk; \
    else \
        ln -sf /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk; \
    fi

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Copier et installer les dépendances depuis requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

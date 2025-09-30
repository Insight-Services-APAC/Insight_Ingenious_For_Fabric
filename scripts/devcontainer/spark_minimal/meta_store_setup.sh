#!/bin/bash

# Check for metastore type environment variable
METASTORE_TYPE="${METASTORE_TYPE:-mysql}"

# Detect architecture for ARM/x64 support
ARCH=$(uname -m)
echo "Detected architecture: $ARCH"
if [[ "$ARCH" == "x86_64" ]]; then
    MYSQL_ARCH="amd64"
    MYSQL_JDBC_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.11/mysql-connector-java-8.0.11.jar"
elif [[ "$ARCH" == "aarch64" ]]; then
    MYSQL_ARCH="arm64"
    MYSQL_JDBC_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.11/mysql-connector-java-8.0.11.jar"
else
    echo "❌ Unsupported architecture: $ARCH"
    exit 1
fi

echo "Setting up ${METASTORE_TYPE} metastore with Delta Lake support..."
echo "Spark Home: $SPARK_HOME"
# Create necessary directories
sudo mkdir -p $SPARK_HOME/conf
sudo mkdir -p /tmp/spark-warehouse
# sudo chown -R 1001:1001 /tmp/spark-warehouse

# Kill any remaining mysqld processes
sudo pkill -f mysqld 2>/dev/null || true
sudo pkill -f mysqld_safe 2>/dev/null || true

# Create mysql user
sudo useradd -r -s /bin/false mysql 2>/dev/null || true

apt-get install lsb-release
apt-get install gnupg

# Download MySQL 8.0 binary
wget -q https://dev.mysql.com/get/mysql-apt-config_0.8.34-1_all.deb

# Install the repository package
sudo DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.34-1_all.deb

# Update package lists
sudo apt-get update

# Install MySQL Server with non-interactive configuration
# Check for metastore type environment variable
METASTORE_TYPE="${METASTORE_TYPE:-mysql}"
echo "Using metastore type: $METASTORE_TYPE"

if [[ "$METASTORE_TYPE" == "postgresql" ]]; then
    echo "Installing PostgreSQL..."

    # Install locales and ensure UTF-8 locale is available
    echo "Setting up locales for PostgreSQL..."
    sudo apt-get update
    sudo apt-get install -y locales

    # Generate en_US.UTF-8 locale if not present
    if ! locale -a | grep -q "en_US.utf8"; then
        echo "en_US.UTF-8 UTF-8" | sudo tee -a /etc/locale.gen
        sudo locale-gen
    fi

    # Set locale environment variables for PostgreSQL
    export LC_ALL=en_US.UTF-8
    export LANG=en_US.UTF-8
    export LANGUAGE=en_US.UTF-8

    # Install PostgreSQL
    sudo apt-get install -y wget gnupg2 lsb-release
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update
    sudo apt-get install -y postgresql-15 postgresql-client-15

    # Configure PostgreSQL
    echo "Configuring PostgreSQL for Hive metastore..."
    sudo -u postgres pg_ctlcluster 15 main start

    # Create metastore database and user with explicit UTF-8 encoding
    sudo -u postgres psql -c "CREATE DATABASE metastore WITH ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;"
    sudo -u postgres psql -c "CREATE USER hive WITH PASSWORD 'hivepassword';"
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;"
    sudo -u postgres psql -c "ALTER DATABASE metastore OWNER TO hive;"

    # Configure PostgreSQL for remote connections (localhost only)
    sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = 'localhost'/" /etc/postgresql/15/main/postgresql.conf
    echo "host    metastore       hive            127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/15/main/pg_hba.conf

    # Restart PostgreSQL
    sudo -u postgres pg_ctlcluster 15 main restart

    # Wait for PostgreSQL to be ready
    echo "Waiting for PostgreSQL to start..."
    MAX_ATTEMPTS=30
    ATTEMPT=1
    while ! pg_isready -h localhost -p 5432 -U postgres &>/dev/null; do
        if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
            echo "❌ PostgreSQL failed to start after $MAX_ATTEMPTS attempts"
            exit 1
        fi
        echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for PostgreSQL to be ready..."
        sleep 2
        ATTEMPT=$((ATTEMPT + 1))
    done

    echo "✅ PostgreSQL is ready!"

    # Download PostgreSQL JDBC driver
    echo "Downloading PostgreSQL JDBC driver..."
    POSTGRES_JDBC_URL="https://jdbc.postgresql.org/download/postgresql-42.7.5.jar"
    wget -q "$POSTGRES_JDBC_URL" -O $SPARK_HOME/jars/postgresql-42.7.5.jar

else
    echo "Installing MySQL Server from apt..."
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server mysql-client
fi


# Configure MySQL for Hive compatibility (only if using MySQL)
if [[ "$METASTORE_TYPE" == "mysql" ]]; then
    echo "Configuring MySQL for Hive metastore..."
    sudo mkdir -p /etc/mysql/mysql.conf.d
    sudo tee /etc/mysql/mysql.conf.d/50-hive-metastore.cnf > /dev/null << 'EOF'
[mysqld]
sql_mode=""
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci

# Large column support (MySQL 8.0+ compatible)
innodb_file_per_table=1
innodb_default_row_format=DYNAMIC
innodb_strict_mode=0
innodb_data_home_dir = /myibdata/
innodb_data_file_path=ibdata1:50M:autoextend

# Connection and performance settings
max_connections=200
max_allowed_packet=1G
innodb_buffer_pool_size=256M
innodb_redo_log_capacity=536870912

# Disable strict modes that cause issues with Hive
sql_mode=""
bind-address=127.0.0.1

EOF

    # Start MySQL service manually (no systemctl in container)
    echo "Starting MySQL service manually..."

    # Completely clean up MySQL data directory and reinstall system tables
    echo "Completely cleaning MySQL data directory..."
    sudo pkill -f mysqld 2>/dev/null || true
    sudo pkill -f mysqld_safe 2>/dev/null || true
    sleep 3

    sudo rm -rf /var/run/mysqld*
    sudo rm -rf /var/lib/mysql*
    sudo rm -rf /var/log/mysql/*
    sudo rm -rf /myibdata*
    sudo rm -rf /var/lib/mysql-files

    # Ensure MySQL directories exist with proper permissions
    sudo mkdir -p /var/run/mysqld
    sudo mkdir -p /var/lib/mysql
    sudo mkdir -p /var/log/mysql
    sudo mkdir -p /myibdata
    sudo mkdir -p /var/lib/mysql-files

    sudo chown mysql:mysql /var/run/mysqld
    sudo chown mysql:mysql /var/lib/mysql
    sudo chown mysql:mysql /var/log/mysql
    sudo chown mysql:mysql /myibdata
    sudo chown mysql:mysql /var/lib/mysql-files


    echo "Initializing MySQL data directory..."
    sudo mysqld --initialize-insecure --user=mysql --datadir=/var/lib/mysql

    # Start MySQL daemon manually
    echo "Starting MySQL daemon..."
    sudo mysqld_safe --user=mysql --datadir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock --pid-file=/var/run/mysqld/mysqld.pid &

    # Wait for MySQL to be ready
    echo "Waiting for MySQL to start..."
    MAX_ATTEMPTS=30
    ATTEMPT=1
    while ! mysqladmin ping --silent 2>/dev/null; do
        if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
            echo "❌ MySQL failed to start after $MAX_ATTEMPTS attempts"
            echo "Checking MySQL processes:"
            ps aux | grep mysqld | grep -v grep
            echo "Checking MySQL logs:"
            sudo tail -20 /var/log/mysql/error.log 2>/dev/null || echo "No error log found"
            exit 1
        fi
        echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for MySQL to be ready..."
        sleep 2
        ATTEMPT=$((ATTEMPT + 1))
    done

    echo "✅ MySQL is ready!"

    # Set root password and create database/user
    echo "Configuring MySQL for metastore..."
    mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'sparkmetastore';" 2>/dev/null || echo "Root user already configured"
    mysql -e "CREATE DATABASE IF NOT EXISTS metastore CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    mysql -e "CREATE USER IF NOT EXISTS 'hive'@'localhost' IDENTIFIED BY 'hivepassword';"
    mysql -e "GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'localhost';"
    # Grant additional system privileges needed for Hive metastore
    mysql -e "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'hive'@'localhost';"
    mysql -e "GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'hive'@'localhost';"
    mysql -e "GRANT SUPER ON *.* TO 'hive'@'localhost';"
    mysql -e "FLUSH PRIVILEGES;"

    # Download MySQL JDBC driver instead of MariaDB
    echo "Downloading MySQL JDBC driver..."
    sudo rm -f $SPARK_HOME/jars/mariadb-java-client-*.jar
    sudo rm -f $SPARK_HOME/jars/mysql-connector-java-*.jar
    wget -q "$MYSQL_JDBC_URL" -O $SPARK_HOME/jars/mysql-connector-java-8.0.11.jar
fi



# Download Delta Lake JARs
echo "Downloading Delta Lake..."
sudo rm -f $SPARK_HOME/jars/delta-spark_*.jar
sudo rm -f $SPARK_HOME/jars/delta-storage-*.jar
wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar -O $SPARK_HOME/jars/delta-spark_2.13-4.0.0.jar
wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar -O $SPARK_HOME/jars/delta-storage-4.0.0.jar

# Configure Spark defaults
echo "Configuring Spark defaults..."
if [[ "$METASTORE_TYPE" == "postgresql" ]]; then
    sudo tee $SPARK_HOME/conf/spark-defaults.conf > /dev/null << 'EOF'
# PostgreSQL Metastore Configuration
spark.sql.catalogImplementation=hive
spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:postgresql://localhost:5432/metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName=org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionUserName=hive
spark.hadoop.javax.jdo.option.ConnectionPassword=hivepassword
EOF
else
    sudo tee $SPARK_HOME/conf/spark-defaults.conf > /dev/null << 'EOF'
# MySQL Metastore Configuration
spark.sql.catalogImplementation=hive
spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
spark.hadoop.javax.jdo.option.ConnectionDriverName=com.mysql.cj.jdbc.Driver
spark.hadoop.javax.jdo.option.ConnectionUserName=hive
spark.hadoop.javax.jdo.option.ConnectionPassword=hivepassword
EOF
fi

# Add common configuration
sudo tee -a $SPARK_HOME/conf/spark-defaults.conf > /dev/null << 'EOF'

# Schema management - prevent duplicate schema creation
spark.hadoop.datanucleus.schema.autoCreateAll=false
spark.hadoop.hive.metastore.schema.verification=false
spark.hadoop.datanucleus.autoCreateSchema=false
spark.hadoop.datanucleus.fixedDatastore=true

# Prevent duplicate index creation
spark.hadoop.datanucleus.autoCreateTables=false
spark.hadoop.datanucleus.autoCreateColumns=false
spark.hadoop.datanucleus.autoCreateConstraints=false

# DataNucleus MySQL-specific settings
spark.hadoop.datanucleus.rdbms.mysql.characterSet=utf8mb4
spark.hadoop.datanucleus.rdbms.mysql.collation=utf8mb4_unicode_ci
spark.hadoop.datanucleus.identifier.case=UpperCase
spark.hadoop.datanucleus.schema.validateTables=false
spark.hadoop.datanucleus.schema.validateColumns=false
spark.hadoop.datanucleus.schema.validateConstraints=false

# Connection pooling
spark.hadoop.datanucleus.connectionPool.maxActive=10
spark.hadoop.datanucleus.connectionPool.maxIdle=5
spark.hadoop.datanucleus.connectionPool.minIdle=1
spark.hadoop.datanucleus.connectionPool.testOnBorrow=true
spark.hadoop.datanucleus.connectionPool.validationQuery=SELECT 1

# Delta Lake Configuration
# spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
# spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
# spark.databricks.delta.retentionDurationCheck.enabled=false
# spark.databricks.delta.vacuum.logging.enabled=true

# Performance settings
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Warehouse configuration
spark.sql.warehouse.dir=/tmp/spark-warehouse
EOF


# Verify the file was created
if [ -f "$SPARK_HOME/conf/spark-defaults.conf" ]; then
    echo "✅ spark-defaults.conf created successfully"
    echo "File size: $(wc -l < "$SPARK_HOME/conf/spark-defaults.conf") lines"
else
    echo "❌ Failed to create spark-defaults.conf"
    exit 1
fi

# Configure Hive site
echo "Configuring Hive site..."
if [[ "$METASTORE_TYPE" == "postgresql" ]]; then
    sudo tee $SPARK_HOME/conf/hive-site.xml > /dev/null << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hivepassword</value>
  </property>
EOF
else
    sudo tee $SPARK_HOME/conf/hive-site.xml > /dev/null << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true&amp;useSSL=false&amp;allowPublicKeyRetrieval=true&amp;serverTimezone=UTC</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hivepassword</value>
  </property>
EOF
fi

# Add common hive-site configuration
sudo tee -a $SPARK_HOME/conf/hive-site.xml > /dev/null << 'EOF'
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/spark-warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value></value>
  </property>
</configuration>
EOF

# Set proper permissions
#sudo chown -R 1001:1001 $SPARK_HOME/conf/
#sudo chmod 644 $SPARK_HOME/conf/spark-defaults.conf
#sudo chmod 644 $SPARK_HOME/conf/hive-site.xml

if [[ "$METASTORE_TYPE" == "postgresql" ]]; then
    echo "PostgreSQL metastore with Delta Lake setup complete!"
    echo "Database: metastore on localhost:5432"
    echo "User: hive / Password: hivepassword"
    echo "Warehouse location: /tmp/spark-warehouse"
    echo "Delta Lake extensions enabled"

    # Test database connection
    echo "Testing database connection..."
    PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "\dt" 2>/dev/null && echo "✅ Database connection successful" || echo "❌ Database connection failed"

    # Verify database encoding
    echo "Verifying database encoding..."
    PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "SELECT current_setting('server_encoding'), current_setting('lc_collate'), current_setting('lc_ctype');" 2>/dev/null
else
    echo "MySQL metastore with Delta Lake setup complete!"
    echo "Database: metastore on localhost:3306"
    echo "User: hive / Password: hivepassword"
    echo "Warehouse location: /tmp/spark-warehouse"
    echo "Delta Lake extensions enabled"

    # Test database connection
    echo "Testing database connection..."
    mysql -u hive -phivepassword -h localhost metastore -e "SHOW TABLES;" 2>/dev/null && echo "✅ Database connection successful" || echo "❌ Database connection failed"
fi

# Test Spark configuration
echo "Testing Spark configuration..."
$SPARK_HOME/bin/spark-sql --version

#Clean Up
rm -f mysql-8.0.33-linux-glibc2.12-x86_64.tar.xz
rm -f mysql-apt-config_0.8.34-1_all.deb

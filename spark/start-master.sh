#!/bin/sh
set -eu

JAVA_HOME_DIR="${JAVA_HOME:-/opt/java/openjdk}"
CACERTS="$JAVA_HOME_DIR/lib/security/cacerts"

for host in repo1.maven.org repos.spark-packages.org; do
    cert_file="/tmp/${host}.pem"

    if echo | openssl s_client -servername "$host" -connect "$host:443" -showcerts 2>/dev/null \
        | openssl x509 -outform PEM > "$cert_file"; then
        keytool -importcert \
            -noprompt \
            -trustcacerts \
            -alias "shopflow-${host}" \
            -file "$cert_file" \
            -keystore "$CACERTS" \
            -storepass changeit >/dev/null 2>&1 || true
    fi
done

exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0

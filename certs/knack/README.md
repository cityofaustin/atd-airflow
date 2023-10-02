# Knack self-signed SSL certificate and key stored here
This directory is available to the docker-compose stack as a named volume. See the volumne definition in docker-compose.yaml for more details.

The certificate stored here as `esb.cert` and `esb.pem` is used as a self-signed certificate for communication with the Enterprise Service Bus (ESB) for the 311 CSR integration.

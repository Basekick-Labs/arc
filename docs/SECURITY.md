# Security Policy

## Supported Versions

Arc Core is currently in **Alpha** status. Security updates are provided for the latest version only.

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

## Reporting a Vulnerability

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities to: **security[at]basekick[dot]net**

You should receive a response within 48 hours. If the issue is confirmed, we will release a patch as soon as possible.

---

## Security Features

### Authentication & Authorization

Arc Core implements token-based authentication:

- **SHA-256 Token Hashing**: All API tokens are hashed before storage
- **Bearer Token Support**: Standard Authorization header authentication
- **Token Expiration**: Automatic enforcement of token expiration dates
- **Rate Limiting**: Protection against brute force attacks (10 requests/minute on token endpoints, 100 requests/minute globally)

### Request Validation

- **Request Size Limits**: Maximum 100MB per request (configurable via `MAX_REQUEST_SIZE_MB`)
- **Input Validation**: Pydantic models for structured endpoints
- **Binary Payload Validation**: Size and format checks on MessagePack/Line Protocol endpoints
- **Content-Type Validation**: Strict content type checking

### Network Security

- **CORS Configuration**: Configurable Cross-Origin Resource Sharing
- **HTTPS Support**: TLS/SSL encryption for production deployments
- **Request ID Tracking**: All requests tracked with unique IDs for audit trails

---

## Security Configuration

### 1. Token Authentication

**Creating Admin Token:**

```bash
# Docker deployment
docker exec -it arc-api python3 -c "
from api.auth import AuthManager
auth = AuthManager(db_path='/data/historian.db')
token = auth.create_token('admin', description='Admin token')
print(f'Token: {token}')
"

# Native deployment
python3 -c "
from api.auth import AuthManager
auth = AuthManager()
token = auth.create_token('admin', description='Admin token')
print(f'Token: {token}')
"
```

**Using Tokens:**

```bash
# All API requests require Bearer token
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" http://localhost:8000/query
```

### 2. Environment Variables

**Required Security Configuration:**

```bash
# Authentication
AUTH_ENABLED=true                 # Enable authentication (default: true)
DEFAULT_API_TOKEN=your-token      # Seed token (optional)

# Rate Limiting
MAX_REQUEST_SIZE_MB=100          # Maximum request size in MB (default: 100)

# CORS
CORS_ORIGINS=http://localhost:3000  # Allowed origins (comma-separated)

# Secrets (NEVER commit to version control)
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key
DB_PATH=/data/historian.db
```

### 3. File Permissions

Protect sensitive configuration files:

```bash
# Set strict permissions on .env file
chmod 600 .env

# Set permissions on database
chmod 600 /data/historian.db

# Ensure config files are not world-readable
chmod 640 *.conf
```

### 4. Network Security

**Production Deployment Checklist:**

- [ ] Enable HTTPS/TLS (use reverse proxy like nginx or Caddy)
- [ ] Configure firewall rules (allow only necessary ports)
- [ ] Use private networks for internal services (MinIO, database)
- [ ] Implement network segmentation
- [ ] Use security groups (AWS) or firewall rules (GCP)

**Example nginx reverse proxy:**

```nginx
server {
    listen 443 ssl http2;
    server_name arc.yourdomain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 5. Secrets Management

**Development:**
- Use `.env` files (never commit to git)
- Add `.env` to `.gitignore`

**Production:**
- Use secrets managers:
  - AWS Secrets Manager
  - HashiCorp Vault
  - Google Secret Manager
  - Azure Key Vault
- Inject secrets as environment variables
- Rotate secrets regularly

**Example with AWS Secrets Manager:**

```bash
# Retrieve secret and start Arc
SECRET=$(aws secretsmanager get-secret-value --secret-id arc-minio-key --query SecretString --output text)
export MINIO_SECRET_KEY=$SECRET
./start.sh
```

### 6. Database Security

**SQLite Security:**

```bash
# Enable encryption at rest (using SQLCipher)
SQLCIPHER_KEY=your-encryption-key

# Restrict database access
chown arc:arc /data/historian.db
chmod 600 /data/historian.db
```

**Regular Backups:**

```bash
# Backup database regularly
sqlite3 /data/historian.db ".backup /backups/historian-$(date +%Y%m%d).db"
```

### 7. MinIO Security

**MinIO Best Practices:**

```bash
# Use strong access keys
MINIO_ACCESS_KEY=$(openssl rand -base64 32)
MINIO_SECRET_KEY=$(openssl rand -base64 32)

# Enable encryption at rest
MINIO_KMS_SECRET_KEY=your-kms-key

# Enable TLS
MINIO_USE_SSL=true
```

**MinIO Security Checklist:**

- [ ] Use strong access keys (min 20 characters)
- [ ] Enable TLS/SSL for MinIO
- [ ] Enable versioning for data protection
- [ ] Configure bucket policies (least privilege)
- [ ] Enable audit logging
- [ ] Use private networks for MinIO access

---

## Security Best Practices

### Development

1. **Never commit secrets** to version control
2. **Use environment variables** for all sensitive data
3. **Test security controls** in staging environment
4. **Review dependencies** regularly for vulnerabilities
5. **Enable debug mode only in development** (`LOG_LEVEL=DEBUG`)

### Production

1. **Use HTTPS/TLS** for all external traffic
2. **Enable authentication** on all endpoints
3. **Implement network segmentation**
4. **Regular security updates** - update Arc Core and dependencies
5. **Monitor and audit** - review logs regularly
6. **Backup regularly** - database and configuration
7. **Principle of least privilege** - minimal permissions for all components
8. **Rate limiting** - prevent abuse and DoS attacks

### Monitoring

Monitor these security events:

- Failed authentication attempts
- Rate limit violations
- Unusual query patterns
- Large data exports
- Configuration changes
- Token creation/deletion

**Example log monitoring:**

```bash
# Monitor failed auth
docker logs arc-api | grep "Invalid token"

# Monitor rate limits
docker logs arc-api | grep "Rate limit exceeded"
```

---

## Compliance

### Data Protection

- **Encryption at Rest**: Use MinIO encryption or encrypted volumes
- **Encryption in Transit**: Enable HTTPS/TLS
- **Access Control**: Token-based authentication with expiration
- **Audit Trail**: Request ID tracking in all logs

### GDPR Considerations

If handling personal data:

1. Implement data retention policies
2. Enable audit logging
3. Provide data export capabilities (CSV endpoints)
4. Implement data deletion workflows
5. Document data processing activities

---

## Known Limitations (Alpha)

Arc Core is in **alpha** status. Current security limitations:

- ✅ Token-based auth (no OAuth/SAML)
- ✅ Basic rate limiting (no advanced DDoS protection)
- ⚠️ No built-in secrets encryption (use external secrets manager)
- ⚠️ No role-based access control (RBAC) - enterprise feature
- ⚠️ No audit logging - enterprise feature
- ⚠️ No multi-factor authentication (MFA)

**These limitations are acceptable for development/testing but require additional controls for production.**

---

## Security Updates

Subscribe to security updates:

- GitHub Security Advisories: Watch the repository
- Email: security[at]basekick[dot]net

---

## Incident Response

If you suspect a security incident:

1. **Isolate**: Stop the Arc Core service
2. **Assess**: Review logs for unauthorized access
3. **Contain**: Rotate all tokens and secrets
4. **Report**: Contact security[at]basekick[dot]net
5. **Recover**: Restore from secure backup if needed
6. **Document**: Record timeline and actions taken

---

## Security Checklist

### Pre-Production

- [ ] All secrets in environment variables or secrets manager
- [ ] HTTPS/TLS enabled
- [ ] Authentication enabled (`AUTH_ENABLED=true`)
- [ ] Strong MinIO access keys generated
- [ ] File permissions set correctly (600 for .env, database)
- [ ] Rate limiting configured
- [ ] Request size limits appropriate
- [ ] Firewall rules configured
- [ ] Monitoring and alerting setup
- [ ] Backup strategy implemented

### Regular Maintenance

- [ ] Review and rotate tokens monthly
- [ ] Update Arc Core to latest version
- [ ] Update dependencies (`pip install -U -r requirements.txt`)
- [ ] Review access logs for anomalies
- [ ] Test backup restoration
- [ ] Audit user access
- [ ] Review and update firewall rules

---

## Contact

- **Security Issues**: security[at]basekick[dot]net
- **General Support**: support[at]basekick[dot]net
- **GitHub Issues**: https://github.com/basekick-labs/arc-core/issues (non-security only)

---

**Last Updated**: October 2025
**Version**: Alpha 0.1.0

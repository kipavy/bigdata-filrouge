#!/bin/bash
set -e

# ============================================
# Bidata Velib ETL - Deployment Script
# ============================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    if ! command -v docker compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi

    log_info "All prerequisites met"
}

# Pull latest code (if git repo)
pull_latest() {
    if [ -d ".git" ]; then
        log_info "Pulling latest code from git..."
        git pull origin main || log_warn "Git pull failed, continuing with local code"
    else
        log_warn "Not a git repository, skipping git pull"
    fi
}

# Backup databases before deployment
backup_databases() {
    log_info "Creating database backups..."

    BACKUP_DIR="./backups"
    mkdir -p "$BACKUP_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)

    # Backup PostgreSQL
    if docker compose ps postgres | grep -q "running"; then
        log_info "Backing up PostgreSQL..."
        docker compose exec -T postgres pg_dump -U airflow airflow > "$BACKUP_DIR/postgres_$TIMESTAMP.sql" 2>/dev/null || \
            log_warn "PostgreSQL backup failed (might be empty database)"
    fi

    # Backup MongoDB
    if docker compose ps mongodb | grep -q "running"; then
        log_info "Backing up MongoDB..."
        docker compose exec -T mongodb mongodump --username=mongo --password=mongo \
            --out=/tmp/mongodump_$TIMESTAMP 2>/dev/null || \
            log_warn "MongoDB backup failed (might be empty database)"
    fi

    log_info "Backups completed"
}

# Deploy services
deploy_services() {
    log_info "Deploying services..."

    # Pull latest images
    log_info "Pulling latest Docker images..."
    docker compose pull

    # Start services
    log_info "Starting services..."
    docker compose up -d --remove-orphans

    log_info "Services deployed"
}

# Wait for services to be healthy
wait_for_health() {
    log_info "Waiting for services to become healthy..."

    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        log_info "Health check attempt $attempt/$max_attempts..."

        # Check Airflow webserver
        if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
            log_info "Airflow webserver is healthy"

            # Check Grafana
            if curl -sf http://localhost:3000/api/health > /dev/null 2>&1; then
                log_info "Grafana is healthy"
                return 0
            fi
        fi

        sleep 10
        attempt=$((attempt + 1))
    done

    log_error "Services did not become healthy in time"
    return 1
}

# Verify DAGs are loaded
verify_dags() {
    log_info "Verifying Airflow DAGs..."

    if docker compose exec -T airflow-webserver airflow dags list 2>/dev/null | grep -q "velib_etl"; then
        log_info "DAG 'velib_etl' is loaded and available"
    else
        log_warn "DAG 'velib_etl' not found - check Airflow logs"
    fi
}

# Cleanup old resources
cleanup() {
    log_info "Cleaning up old resources..."

    # Remove old Docker images
    docker image prune -f > /dev/null 2>&1 || true

    # Keep only last 5 backups
    if [ -d "./backups" ]; then
        cd ./backups
        ls -t postgres_*.sql 2>/dev/null | tail -n +6 | xargs -r rm --
        cd ..
    fi

    log_info "Cleanup completed"
}

# Print service status
print_status() {
    echo ""
    echo "============================================"
    echo "Deployment Complete!"
    echo "============================================"
    echo ""
    docker compose ps
    echo ""
    echo "Service URLs:"
    echo "  Airflow:  http://localhost:8080  (airflow/airflow)"
    echo "  Grafana:  http://localhost:3000  (admin/admin)"
    echo ""
}

# Rollback function
rollback() {
    log_error "Deployment failed! Rolling back..."

    # Restore from latest backup if available
    LATEST_BACKUP=$(ls -t ./backups/postgres_*.sql 2>/dev/null | head -1)
    if [ -n "$LATEST_BACKUP" ]; then
        log_info "Restoring from $LATEST_BACKUP..."
        docker compose exec -T postgres psql -U airflow airflow < "$LATEST_BACKUP" || true
    fi

    # Restart services
    docker compose down
    docker compose up -d

    log_error "Rollback completed. Please check service status."
    exit 1
}

# Main deployment flow
main() {
    log_info "Starting Bidata Velib ETL deployment..."

    check_prerequisites
    pull_latest

    # Backup if services are running
    if docker compose ps 2>/dev/null | grep -q "running"; then
        backup_databases
    fi

    deploy_services

    if ! wait_for_health; then
        rollback
    fi

    verify_dags
    cleanup
    print_status

    log_info "Deployment successful!"
}

# Run main function
main "$@"

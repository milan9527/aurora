#!/bin/bash

# Set database connection parameters
DB_HOST="apg-sin-crosstest.cluster-chxq9micv2au.ap-southeast-1.rds.amazonaws.com"
DB_USER="postgres"
DB_PASSWORD="password"
DB_NAME="postgres"

# Check if LSN is provided as argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <LSN>"
    echo "Example: $0 3/5A0395E0"
    exit 1
fi

# LSN to check (from command line argument)
TARGET_LSN="$1"

# Validate LSN format
if ! [[ $TARGET_LSN =~ ^[0-9]+/[0-9A-F]+$ ]]; then
    echo "Error: Invalid LSN format. LSN should be in format like '3/5A0395E0'"
    exit 1
fi

echo "Checking LSN: $TARGET_LSN"

# Get current WAL position
echo -e "\nCurrent WAL position:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());"

# Get replication slot information
echo -e "\nReplication slot information:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'replication_slot';"

# Compare LSN positions
echo -e "\nComparing LSN positions:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT
    '$TARGET_LSN'::pg_lsn AS target_lsn,
    restart_lsn,
    confirmed_flush_lsn,
    '$TARGET_LSN'::pg_lsn < restart_lsn AS is_before_restart,
    '$TARGET_LSN'::pg_lsn BETWEEN restart_lsn AND confirmed_flush_lsn AS is_in_range
FROM
    pg_replication_slots
WHERE
    slot_name = 'replication_slot';"

# Check if we can create a temporary replication slot to check older LSNs
echo -e "\nAttempting to create a temporary replication slot for checking:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
DO \$\$
BEGIN
    BEGIN
        PERFORM pg_create_logical_replication_slot('temp_check_slot', 'test_decoding', false);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not create temporary slot: %', SQLERRM;
    END;
END \$\$;"

# If temporary slot was created, check LSN availability
echo -e "\nChecking if temporary slot was created:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT slot_name, restart_lsn FROM pg_replication_slots WHERE slot_name = 'temp_check_slot';"

# Clean up temporary slot if it exists
echo -e "\nCleaning up temporary slot if it exists:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
DO \$\$
BEGIN
    BEGIN
        PERFORM pg_drop_replication_slot('temp_check_slot');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not drop temporary slot: %', SQLERRM;
    END;
END \$\$;"

# Provide conclusion
echo -e "\nConclusion:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
DO \$\$
DECLARE
    target_lsn pg_lsn := '$TARGET_LSN'::pg_lsn;
    current_lsn pg_lsn;
    restart_lsn pg_lsn;
    confirmed_lsn pg_lsn;
    is_available boolean;
BEGIN
    SELECT pg_current_wal_lsn() INTO current_lsn;

    SELECT rs.restart_lsn, rs.confirmed_flush_lsn
    INTO restart_lsn, confirmed_lsn
    FROM pg_replication_slots rs
    WHERE slot_name = 'replication_slot';

    is_available := target_lsn BETWEEN restart_lsn AND current_lsn;

    RAISE NOTICE 'Target LSN: %', target_lsn;
    RAISE NOTICE 'Current WAL position: %', current_lsn;
    RAISE NOTICE 'Replication slot restart_lsn: %', restart_lsn;
    RAISE NOTICE 'Replication slot confirmed_flush_lsn: %', confirmed_lsn;

    IF target_lsn < restart_lsn THEN
        RAISE NOTICE 'The target LSN % is BEFORE the replication slot restart_lsn %. This LSN is NO LONGER AVAILABLE in the slot.', target_lsn, restart_lsn;
    ELSIF target_lsn BETWEEN restart_lsn AND confirmed_lsn THEN
        RAISE NOTICE 'The target LSN % is BETWEEN restart_lsn % and confirmed_flush_lsn %. This LSN is AVAILABLE in the slot.', target_lsn, restart_lsn, confirmed_lsn;
    ELSIF target_lsn BETWEEN confirmed_lsn AND current_lsn THEN
        RAISE NOTICE 'The target LSN % is BETWEEN confirmed_flush_lsn % and current_lsn %. This LSN is AVAILABLE in the WAL but may not be consumed by the slot yet.', target_lsn, confirmed_lsn, current_lsn;
    ELSE
        RAISE NOTICE 'The target LSN % is AFTER the current WAL position %. This LSN does NOT EXIST yet.', target_lsn, current_lsn;
    END IF;
END \$\$;"

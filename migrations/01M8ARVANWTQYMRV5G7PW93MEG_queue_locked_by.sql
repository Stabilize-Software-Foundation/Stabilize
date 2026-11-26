-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE queue_messages ADD COLUMN IF NOT EXISTS locked_by TEXT;

-- migrate: down
SELECT 1;

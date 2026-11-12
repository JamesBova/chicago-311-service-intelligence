-- Chicago 311 Service Intelligence: Gold schema
-- Run after the Silver schema script. Does not modify existing Bronze objects.

CREATE SCHEMA IF NOT EXISTS CHICAGO_311_DEV.GOLD
    COMMENT = 'Analytics-ready Chicago 311 dimensions, facts and daily metrics';
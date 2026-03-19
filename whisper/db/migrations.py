import psycopg2
from psycopg2 import extensions
from db.connection import get_connection

import logging
logger = logging.getLogger("btt_root_logger")


def initDatabase():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        logger.info("Initialized database connection.")
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur.execute(
            """
            CREATE EXTENSION IF NOT EXISTS pg_trgm;

            CREATE TABLE IF NOT EXISTS series_data (
                series_id INTEGER PRIMARY KEY,
                series_name VARCHAR(255),
                lecturer_ids INTEGER[]
            );
            CREATE TABLE IF NOT EXISTS lecturer_data (
                lecturer_id INTEGER PRIMARY KEY,
                lecturer_name VARCHAR(255) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lecture_data (
                lecture_id INTEGER PRIMARY KEY, 
                language VARCHAR(50),
                date DATE,
                lecturer_ids INTEGER[],
                series_id INTEGER,
                semester VARCHAR(50),
                duration INTERVAL,
                title VARCHAR(255),
                video_mp4 VARCHAR(255),
                desktop_mp4 VARCHAR(255),
                podcast_mp4 VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS vtt_files (
                id SERIAL PRIMARY KEY,
                lecture_id INTEGER NOT NULL,
                language VARCHAR(50) NOT NULL,
                is_original_lang BOOLEAN NOT NULL,
                vtt_data BYTEA NOT NULL,
                txt_data BYTEA NOT NULL,
                asr_model VARCHAR(255),
                compute_type VARCHAR(255),
                creation_date TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS vtt_lines (
                id BIGSERIAL PRIMARY KEY,
                vtt_file_id INTEGER NOT NULL REFERENCES vtt_files(id) ON DELETE CASCADE,
                series_id INTEGER NOT NULL REFERENCES series_data(series_id) ON DELETE CASCADE,
                language VARCHAR(50) NOT NULL,
                lecturer_ids INTEGER[] NOT NULL,
                line_number INTEGER NOT NULL,
                ts_start INTEGER NOT NULL,
                ts_end INTEGER NOT NULL,
                content TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                api_key VARCHAR(255) UNIQUE NOT NULL,
                person_name VARCHAR(255),
                person_email VARCHAR(255),
                creation_date TIMESTAMPTZ DEFAULT NOW(),
                expiration_date TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '3 months'),
                status VARCHAR(255) DEFAULT 'active'
            );
            CREATE TABLE IF NOT EXISTS blacklist_ids (
                lecture_id INTEGER PRIMARY KEY,
                reason VARCHAR(255),
                times_tried INTEGER DEFAULT 1,
                creation_date TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_vtt_files_lecture_id ON vtt_files (lecture_id);
            CREATE INDEX IF NOT EXISTS idx_api_keys_api_key ON api_keys (api_key);

            CREATE INDEX IF NOT EXISTS idx_lines_trgm ON vtt_lines USING gin (content gin_trgm_ops);
            CREATE INDEX IF NOT EXISTS idx_lines_series_id ON vtt_lines (series_id);
            CREATE INDEX IF NOT EXISTS idx_lines_lecture_id ON vtt_lines (vtt_file_id);
            CREATE INDEX IF NOT EXISTS idx_lines_language ON vtt_lines (language);

            """)

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def clearDatabase():
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(
            """
            DROP TABLE IF EXISTS "vtt_lines";
            DROP TABLE IF EXISTS "vtt_files";
            DROP TABLE IF EXISTS "lecturer_data";
            DROP TABLE IF EXISTS "series_data";
            DROP TABLE IF EXISTS "lecture_data";
            DROP TABLE IF EXISTS "api_keys";
            DROP TABLE IF EXISTS "blacklist_ids";
        """
        )
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

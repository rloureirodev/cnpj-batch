CREATE TABLE if not exists controle_processos (
    id BIGSERIAL PRIMARY KEY,

    job_name VARCHAR(200) NOT NULL,
    ano_mes VARCHAR(10),

    etapa VARCHAR(100) NOT NULL, -- ex: DOWNLOAD, UNZIP, COPY, HASH, SYNC, CLEANUP

    status VARCHAR(20) NOT NULL, -- RUNNING | OK | ERROR | SKIPPED

    start_time TIMESTAMP NOT NULL DEFAULT now(),
    end_time TIMESTAMP,

    error_message TEXT,
    error_stack TEXT,

    zip_file VARCHAR(500),
    csv_file VARCHAR(500),
    tabela_stage VARCHAR(200),

    hash VARCHAR(200)
);

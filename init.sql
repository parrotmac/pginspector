CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE person (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    name varchar(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE manufacturer (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    name varchar(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE model (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    name varchar(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE vehicle (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    make uuid NOT NULL REFERENCES manufacturer(id),
    model uuid NOT NULL REFERENCES model(id),
    year integer NOT NULL,
    vin varchar(255) NOT NULL
);

CREATE TABLE ownership (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    person uuid NOT NULL REFERENCES person(id),
    vehicle uuid NOT NULL REFERENCES vehicle(id),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP
);

CREATE TABLE rental (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    vehicle uuid NOT NULL REFERENCES vehicle(id),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP
);

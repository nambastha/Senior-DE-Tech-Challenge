CREATE TABLE members (
    membership_id varchar(255) NOT NULL PRIMARY KEY,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    address1 varchar(255) NOT NULL,
    city varchar(125) NOT NULL,
    country varchar(125) NOT NULL,
    postal_code varchar(10) NOT NULL,
    country varchar(10) NOT NULL
);

CREATE TABLE orders (
    order_id varchar(255) NOT NULL PRIMARY KEY,
    membership_id varchar(255)  NOT NULL,
    order_date date NOT NULL,
    items_bought int NOT NULL,
    total_items_weight int NOT NULL,
    total_cost int NOT NULL
);

CREATE TABLE shipments (
    shipment_id varchar(255) NOT NULL PRIMARY KEY,
    order_id varchar(255) NOT NULL,
    shipment_date date NOT NULL
);

CREATE TABLE items (
    item_id varchar(255) NOT NULL PRIMARY KEY,
    item_name varchar(100) NOT NULL,
    manufacturer varchar(100) NOT NULL,
    cost int NOT NULL,
    weight int NOT NULL,
    quantity int NOT NULL
);

CREATE TABLE order_items (
    id varchar(255) NOT NULL PRIMARY KEY,
    order_id varchar(255) NOT NULL,
    item_id varchar(255) NOT NULL,
    quantity int NOT NULL,
    cost_each int NOT NULL,
    weight_each int NOT NULL
);
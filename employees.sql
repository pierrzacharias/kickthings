CREATE TABLE IF NOT EXISTS employees (
    company_id varchar(150) NOT NULL,
    id double NOT NULL,
    score_affiliation INT(20),
    canonical_shorthand_name  varchar(255) DEFAULT NULL,
    canonical_shorthand_name_hash varchar(255) DEFAULT NULL,
    canonical_url varchar(255) DEFAULT NULL,
    connections varchar(255) DEFAULT NULL,
    connections_count double  DEFAULT NULL,
    country varchar(255) DEFAULT NULL,
    created date DEFAULT NULL,
    deleted double  DEFAULT NULL,
    experience_count  double  DEFAULT NULL,
    experiences_by_months double  DEFAULT NULL,
    hash  varchar(255) DEFAULT NULL,
    industry  varchar(255) DEFAULT NULL,
    last_response_code  double  DEFAULT NULL,
    last_updated  date DEFAULT NULL ,
    last_updated_ux double  DEFAULT NULL,
    location  varchar(255) DEFAULT NULL,
    logo_url  varchar(255) DEFAULT NULL,
    member_shorthand_name varchar(255) DEFAULT NULL,
    member_shorthand_name_hash  varchar(255) DEFAULT NULL,
    name  varchar(255) DEFAULT NULL,
    outdated  double  DEFAULT NULL,
    recommendations_count varchar(255) DEFAULT NULL,
    summary varchar(255) DEFAULT NULL,
    title varchar(255) DEFAULT NULL,
    url varchar(255) DEFAULT NULL,
    PRIMARY KEY (id,company_id), 
    FOREIGN KEY (company_id)
    REFERENCES entreprise_pb(id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

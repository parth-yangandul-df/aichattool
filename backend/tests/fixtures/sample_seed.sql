-- =============================================================================
-- IFRS 9 Banking Sample Database
-- Expected Credit Loss (ECL) provisioning, staging, and impairment data
-- =============================================================================

-- Counterparties (borrowers)
CREATE TABLE counterparties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    segment VARCHAR(50) NOT NULL CHECK (segment IN ('retail', 'corporate', 'sme')),
    country VARCHAR(100) NOT NULL,
    credit_rating VARCHAR(10) NOT NULL,
    is_defaulted BOOLEAN DEFAULT FALSE,
    industry VARCHAR(100),
    onboarding_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE counterparties IS 'Bank customers and borrowers subject to IFRS 9 reporting';
COMMENT ON COLUMN counterparties.segment IS 'Customer segment: retail, corporate, sme';
COMMENT ON COLUMN counterparties.credit_rating IS 'Internal credit rating grade (AAA to D)';
COMMENT ON COLUMN counterparties.is_defaulted IS 'Whether the counterparty is currently in default';

-- Facilities (loan accounts)
CREATE TABLE facilities (
    id SERIAL PRIMARY KEY,
    counterparty_id INTEGER NOT NULL REFERENCES counterparties(id),
    facility_type VARCHAR(50) NOT NULL CHECK (facility_type IN ('mortgage', 'corporate_loan', 'consumer_loan', 'credit_card', 'overdraft')),
    currency VARCHAR(3) NOT NULL DEFAULT 'EUR',
    origination_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    interest_rate NUMERIC(5, 2) NOT NULL,
    credit_limit NUMERIC(15, 2) NOT NULL,
    outstanding_balance NUMERIC(15, 2) NOT NULL DEFAULT 0,
    is_revolving BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE facilities IS 'Loan facilities and credit accounts';
COMMENT ON COLUMN facilities.facility_type IS 'Type of credit facility: mortgage, corporate_loan, consumer_loan, credit_card, overdraft';
COMMENT ON COLUMN facilities.credit_limit IS 'Maximum approved credit limit';
COMMENT ON COLUMN facilities.outstanding_balance IS 'Current drawn balance';
COMMENT ON COLUMN facilities.is_revolving IS 'Whether the facility is revolving (e.g. credit card, overdraft)';

-- Exposures (monthly snapshots for IFRS 9 reporting)
CREATE TABLE exposures (
    id SERIAL PRIMARY KEY,
    facility_id INTEGER NOT NULL REFERENCES facilities(id),
    reporting_date DATE NOT NULL,
    ead NUMERIC(15, 2) NOT NULL,
    carrying_amount NUMERIC(15, 2) NOT NULL,
    stage INTEGER NOT NULL CHECK (stage IN (1, 2, 3)),
    days_past_due INTEGER DEFAULT 0,
    off_balance_amount NUMERIC(15, 2) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE exposures IS 'Monthly exposure snapshots used for IFRS 9 ECL calculation';
COMMENT ON COLUMN exposures.ead IS 'Exposure at Default — total exposure amount including off-balance';
COMMENT ON COLUMN exposures.carrying_amount IS 'On-balance sheet carrying amount (gross)';
COMMENT ON COLUMN exposures.stage IS 'IFRS 9 stage: 1=Performing, 2=SICR (Significant Increase in Credit Risk), 3=Credit-Impaired';
COMMENT ON COLUMN exposures.days_past_due IS 'Number of days the payment is overdue';

-- ECL Provisions (expected credit loss calculations)
CREATE TABLE ecl_provisions (
    id SERIAL PRIMARY KEY,
    exposure_id INTEGER NOT NULL REFERENCES exposures(id),
    reporting_date DATE NOT NULL,
    pd_12m NUMERIC(8, 6) NOT NULL,
    pd_lifetime NUMERIC(8, 6) NOT NULL,
    lgd NUMERIC(8, 6) NOT NULL,
    ecl_12m NUMERIC(15, 2) NOT NULL,
    ecl_lifetime NUMERIC(15, 2) NOT NULL,
    provision_amount NUMERIC(15, 2) NOT NULL,
    stage INTEGER NOT NULL CHECK (stage IN (1, 2, 3)),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE ecl_provisions IS 'Expected Credit Loss provisions per exposure snapshot';
COMMENT ON COLUMN ecl_provisions.pd_12m IS '12-month Probability of Default';
COMMENT ON COLUMN ecl_provisions.pd_lifetime IS 'Lifetime Probability of Default';
COMMENT ON COLUMN ecl_provisions.lgd IS 'Loss Given Default rate';
COMMENT ON COLUMN ecl_provisions.ecl_12m IS '12-month Expected Credit Loss amount';
COMMENT ON COLUMN ecl_provisions.ecl_lifetime IS 'Lifetime Expected Credit Loss amount';
COMMENT ON COLUMN ecl_provisions.provision_amount IS 'Final booked provision (12m for Stage 1, lifetime for Stage 2/3)';

-- Collateral
CREATE TABLE collateral (
    id SERIAL PRIMARY KEY,
    facility_id INTEGER NOT NULL REFERENCES facilities(id),
    collateral_type VARCHAR(50) NOT NULL CHECK (collateral_type IN ('property', 'cash', 'guarantee', 'securities')),
    collateral_value NUMERIC(15, 2) NOT NULL,
    valuation_date DATE NOT NULL,
    haircut_pct NUMERIC(5, 2) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE collateral IS 'Collateral pledged against credit facilities';
COMMENT ON COLUMN collateral.collateral_type IS 'Type of collateral: property, cash, guarantee, securities';
COMMENT ON COLUMN collateral.haircut_pct IS 'Valuation haircut percentage applied for LGD calculation';

-- Staging History (audit trail of stage transitions)
CREATE TABLE staging_history (
    id SERIAL PRIMARY KEY,
    facility_id INTEGER NOT NULL REFERENCES facilities(id),
    from_stage INTEGER NOT NULL CHECK (from_stage IN (1, 2, 3)),
    to_stage INTEGER NOT NULL CHECK (to_stage IN (1, 2, 3)),
    reason VARCHAR(50) NOT NULL CHECK (reason IN ('origination', 'upgrade', 'downgrade', 'cure', 'default')),
    effective_date DATE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE staging_history IS 'IFRS 9 stage transition audit trail';
COMMENT ON COLUMN staging_history.from_stage IS 'Previous IFRS 9 stage';
COMMENT ON COLUMN staging_history.to_stage IS 'New IFRS 9 stage';
COMMENT ON COLUMN staging_history.reason IS 'Reason for stage change: origination, upgrade, downgrade, cure, default';

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- Counterparties (20 borrowers)
INSERT INTO counterparties (name, segment, country, credit_rating, is_defaulted, industry, onboarding_date) VALUES
('Müller Immobilien GmbH',       'corporate', 'Germany',     'A',   FALSE, 'Real Estate',    '2018-03-15'),
('Jean Dupont',                   'retail',    'France',      'BBB', FALSE, NULL,             '2019-07-22'),
('Schmidt & Partner AG',          'corporate', 'Germany',     'AA',  FALSE, 'Manufacturing',  '2017-01-10'),
('Maria Silva',                   'retail',    'Portugal',    'BB',  FALSE, NULL,             '2020-11-05'),
('Bäckerei Hofmann KG',           'sme',       'Austria',     'BBB', FALSE, 'Food & Beverage','2019-04-18'),
('Alessandro Rossi',              'retail',    'Italy',       'B',   TRUE,  NULL,             '2018-09-30'),
('NordTech Solutions Oy',         'corporate', 'Finland',     'A',   FALSE, 'Technology',     '2020-02-14'),
('Pilar García',                  'retail',    'Spain',       'BBB', FALSE, NULL,             '2021-06-01'),
('Van den Berg Transport BV',     'sme',       'Netherlands', 'BB',  FALSE, 'Logistics',      '2019-08-25'),
('Stavros Papadopoulos',          'retail',    'Greece',      'CCC', TRUE,  NULL,             '2017-12-03'),
('Celtic Engineering Ltd',        'corporate', 'Ireland',     'A',   FALSE, 'Engineering',    '2018-05-20'),
('Kowalski Budownictwo Sp.',      'sme',       'Poland',      'BBB', FALSE, 'Construction',   '2020-09-12'),
('Anna Johansson',                'retail',    'Sweden',      'AA',  FALSE, NULL,             '2021-01-15'),
('BelgaFarm SPRL',                'sme',       'Belgium',     'BB',  FALSE, 'Agriculture',    '2019-11-08'),
('Dimitrov Holdings AD',          'corporate', 'Bulgaria',    'B',   FALSE, 'Holdings',       '2020-04-22'),
('Sophie Laurent',                'retail',    'France',      'A',   FALSE, NULL,             '2022-03-10'),
('TechBridge Kft',                'sme',       'Hungary',     'BBB', FALSE, 'Technology',     '2021-07-19'),
('Erik Andersen',                 'retail',    'Denmark',     'AA',  FALSE, NULL,             '2020-08-05'),
('MedGroup SRL',                  'corporate', 'Romania',     'BB',  FALSE, 'Healthcare',     '2019-06-14'),
('Lukas Novák',                   'retail',    'Czech Republic','B', TRUE,  NULL,             '2018-10-27');

-- Facilities (25 loan facilities)
INSERT INTO facilities (counterparty_id, facility_type, currency, origination_date, maturity_date, interest_rate, credit_limit, outstanding_balance, is_revolving) VALUES
(1,  'corporate_loan', 'EUR', '2018-06-01', '2028-06-01', 2.50, 5000000.00, 3200000.00, FALSE),
(2,  'mortgage',        'EUR', '2019-09-01', '2049-09-01', 1.80, 250000.00,  210000.00,  FALSE),
(3,  'corporate_loan', 'EUR', '2017-03-01', '2027-03-01', 2.10, 10000000.00, 6500000.00, FALSE),
(4,  'consumer_loan',  'EUR', '2021-01-15', '2026-01-15', 5.50, 30000.00,   22000.00,   FALSE),
(5,  'overdraft',       'EUR', '2019-06-01', '2025-06-01', 6.00, 100000.00,  45000.00,   TRUE),
(6,  'mortgage',        'EUR', '2019-01-01', '2049-01-01', 2.00, 180000.00,  165000.00,  FALSE),
(6,  'credit_card',    'EUR', '2020-03-01', '2025-03-01', 18.00, 5000.00,   4800.00,    TRUE),
(7,  'corporate_loan', 'EUR', '2020-06-01', '2030-06-01', 2.80, 3000000.00, 2100000.00, FALSE),
(8,  'consumer_loan',  'EUR', '2021-08-01', '2026-08-01', 4.50, 15000.00,   11000.00,   FALSE),
(9,  'corporate_loan', 'EUR', '2020-01-01', '2025-12-01', 3.50, 500000.00,  380000.00,  FALSE),
(10, 'mortgage',        'EUR', '2018-03-01', '2048-03-01', 2.20, 120000.00,  115000.00,  FALSE),
(10, 'consumer_loan',  'EUR', '2019-05-01', '2024-05-01', 7.00, 10000.00,   9500.00,    FALSE),
(11, 'corporate_loan', 'EUR', '2018-09-01', '2028-09-01', 2.30, 8000000.00, 5000000.00, FALSE),
(12, 'overdraft',       'EUR', '2020-11-01', '2025-11-01', 5.50, 200000.00,  120000.00,  TRUE),
(13, 'mortgage',        'EUR', '2021-03-01', '2051-03-01', 1.50, 400000.00,  385000.00,  FALSE),
(14, 'corporate_loan', 'EUR', '2020-01-15', '2025-01-15', 4.00, 300000.00,  180000.00,  FALSE),
(15, 'corporate_loan', 'EUR', '2020-07-01', '2027-07-01', 4.50, 2000000.00, 1800000.00, FALSE),
(16, 'consumer_loan',  'EUR', '2022-05-01', '2027-05-01', 3.80, 20000.00,   18000.00,   FALSE),
(17, 'overdraft',       'EUR', '2021-09-01', '2026-09-01', 5.00, 150000.00,  60000.00,   TRUE),
(18, 'mortgage',        'EUR', '2020-10-01', '2050-10-01', 1.60, 350000.00,  330000.00,  FALSE),
(19, 'corporate_loan', 'EUR', '2019-09-01', '2026-09-01', 3.20, 1500000.00, 1100000.00, FALSE),
(20, 'consumer_loan',  'EUR', '2019-02-01', '2024-02-01', 6.50, 8000.00,    7200.00,    FALSE),
(1,  'credit_card',    'EUR', '2019-01-01', '2025-01-01', 16.00, 50000.00,  12000.00,   TRUE),
(3,  'overdraft',       'EUR', '2018-01-01', '2026-01-01', 4.50, 2000000.00, 500000.00,  TRUE),
(9,  'consumer_loan',  'EUR', '2020-06-15', '2025-06-15', 5.00, 25000.00,   18000.00,   FALSE);

-- Exposures (reporting date: 2024-12-31) — one per facility
INSERT INTO exposures (facility_id, reporting_date, ead, carrying_amount, stage, days_past_due, off_balance_amount) VALUES
(1,  '2024-12-31', 3500000.00, 3200000.00, 1,  0,  300000.00),
(2,  '2024-12-31', 212000.00,  210000.00,  1,  0,  2000.00),
(3,  '2024-12-31', 7000000.00, 6500000.00, 1,  0,  500000.00),
(4,  '2024-12-31', 22500.00,   22000.00,   2, 45,  500.00),
(5,  '2024-12-31', 85000.00,   45000.00,   1,  0,  40000.00),
(6,  '2024-12-31', 168000.00,  165000.00,  3, 120, 3000.00),
(7,  '2024-12-31', 5000.00,    4800.00,    3, 180, 200.00),
(8,  '2024-12-31', 2200000.00, 2100000.00, 1,  0,  100000.00),
(9,  '2024-12-31', 11200.00,   11000.00,   1,  0,  200.00),
(10, '2024-12-31', 395000.00,  380000.00,  2, 35,  15000.00),
(11, '2024-12-31', 118000.00,  115000.00,  3, 210, 3000.00),
(12, '2024-12-31', 9800.00,    9500.00,    3, 95,  300.00),
(13, '2024-12-31', 5200000.00, 5000000.00, 1,  0,  200000.00),
(14, '2024-12-31', 175000.00,  120000.00,  1,  0,  55000.00),
(15, '2024-12-31', 386000.00,  385000.00,  1,  0,  1000.00),
(16, '2024-12-31', 185000.00,  180000.00,  2, 60,  5000.00),
(17, '2024-12-31', 1850000.00, 1800000.00, 2, 40,  50000.00),
(18, '2024-12-31', 18200.00,   18000.00,   1,  0,  200.00),
(19, '2024-12-31', 130000.00,  60000.00,   1,  0,  70000.00),
(20, '2024-12-31', 332000.00,  330000.00,  1,  0,  2000.00),
(21, '2024-12-31', 1150000.00, 1100000.00, 2, 55,  50000.00),
(22, '2024-12-31', 7500.00,    7200.00,    3, 150, 300.00),
(23, '2024-12-31', 45000.00,   12000.00,   1,  0,  33000.00),
(24, '2024-12-31', 1800000.00, 500000.00,  1,  0,  1300000.00),
(25, '2024-12-31', 18500.00,   18000.00,   2, 30,  500.00);

-- ECL Provisions (one per exposure)
INSERT INTO ecl_provisions (exposure_id, reporting_date, pd_12m, pd_lifetime, lgd, ecl_12m, ecl_lifetime, provision_amount, stage) VALUES
(1,  '2024-12-31', 0.005000, 0.035000, 0.450000, 7875.00,   55125.00,   7875.00,   1),
(2,  '2024-12-31', 0.008000, 0.060000, 0.200000, 339.20,    2544.00,    339.20,    1),
(3,  '2024-12-31', 0.003000, 0.025000, 0.450000, 9450.00,   78750.00,   9450.00,   1),
(4,  '2024-12-31', 0.050000, 0.180000, 0.550000, 618.75,    2227.50,    2227.50,   2),
(5,  '2024-12-31', 0.012000, 0.070000, 0.600000, 612.00,    3570.00,    612.00,    1),
(6,  '2024-12-31', 0.250000, 0.650000, 0.350000, 14700.00,  38220.00,   38220.00,  3),
(7,  '2024-12-31', 0.300000, 0.750000, 0.800000, 1200.00,   3000.00,    3000.00,   3),
(8,  '2024-12-31', 0.004000, 0.030000, 0.450000, 3960.00,   29700.00,   3960.00,   1),
(9,  '2024-12-31', 0.010000, 0.065000, 0.500000, 56.00,     364.00,     56.00,     1),
(10, '2024-12-31', 0.040000, 0.150000, 0.600000, 9480.00,   35550.00,   35550.00,  2),
(11, '2024-12-31', 0.350000, 0.800000, 0.300000, 12390.00,  28320.00,   28320.00,  3),
(12, '2024-12-31', 0.280000, 0.700000, 0.750000, 2058.00,   5145.00,    5145.00,   3),
(13, '2024-12-31', 0.003500, 0.028000, 0.400000, 7280.00,   58240.00,   7280.00,   1),
(14, '2024-12-31', 0.015000, 0.080000, 0.550000, 1443.75,   7700.00,    1443.75,   1),
(15, '2024-12-31', 0.006000, 0.045000, 0.200000, 463.20,    3474.00,    463.20,    1),
(16, '2024-12-31', 0.055000, 0.200000, 0.550000, 5588.75,   20350.00,   20350.00,  2),
(17, '2024-12-31', 0.045000, 0.170000, 0.500000, 41625.00,  157250.00,  157250.00, 2),
(18, '2024-12-31', 0.007000, 0.050000, 0.500000, 63.70,     455.00,     63.70,     1),
(19, '2024-12-31', 0.010000, 0.060000, 0.600000, 780.00,    4680.00,    780.00,    1),
(20, '2024-12-31', 0.005500, 0.040000, 0.200000, 365.20,    2656.00,    365.20,    1),
(21, '2024-12-31', 0.038000, 0.160000, 0.500000, 21850.00,  92000.00,   92000.00,  2),
(22, '2024-12-31', 0.320000, 0.780000, 0.800000, 1920.00,   4680.00,    4680.00,   3),
(23, '2024-12-31', 0.006000, 0.040000, 0.450000, 121.50,    810.00,     121.50,    1),
(24, '2024-12-31', 0.004000, 0.030000, 0.400000, 2880.00,   21600.00,   2880.00,   1),
(25, '2024-12-31', 0.042000, 0.165000, 0.550000, 427.35,    1679.63,    1679.63,   2);

-- Collateral (linked to facilities)
INSERT INTO collateral (facility_id, collateral_type, collateral_value, valuation_date, haircut_pct) VALUES
(1,  'property',   4200000.00, '2024-06-15', 20.00),
(2,  'property',   320000.00,  '2024-03-10', 15.00),
(3,  'securities', 8000000.00, '2024-09-30', 10.00),
(3,  'guarantee',  2000000.00, '2024-01-15', 5.00),
(6,  'property',   200000.00,  '2023-11-20', 20.00),
(8,  'property',   2800000.00, '2024-07-01', 15.00),
(10, 'guarantee',  200000.00,  '2024-04-10', 5.00),
(11, 'property',   160000.00,  '2022-08-15', 25.00),
(13, 'property',   6500000.00, '2024-05-20', 15.00),
(13, 'securities', 3000000.00, '2024-10-01', 10.00),
(15, 'property',   520000.00,  '2024-02-28', 15.00),
(17, 'cash',       500000.00,  '2024-12-31', 0.00),
(20, 'property',   450000.00,  '2024-04-15', 15.00),
(21, 'property',   1400000.00, '2024-08-10', 20.00);

-- Staging History (stage transitions)
INSERT INTO staging_history (facility_id, from_stage, to_stage, reason, effective_date, notes) VALUES
(1,  1, 1, 'origination', '2018-06-01', 'Initial origination'),
(2,  1, 1, 'origination', '2019-09-01', 'Initial origination'),
(3,  1, 1, 'origination', '2017-03-01', 'Initial origination'),
(4,  1, 1, 'origination', '2021-01-15', 'Initial origination'),
(4,  1, 2, 'downgrade',   '2024-09-15', 'Payment 30+ DPD, rating downgrade to BB'),
(5,  1, 1, 'origination', '2019-06-01', 'Initial origination'),
(6,  1, 1, 'origination', '2019-01-01', 'Initial origination'),
(6,  1, 2, 'downgrade',   '2024-03-10', 'Missed 2 consecutive payments'),
(6,  2, 3, 'default',     '2024-07-15', 'Borrower declared insolvent, 90+ DPD'),
(7,  1, 1, 'origination', '2020-03-01', 'Initial origination'),
(7,  1, 3, 'default',     '2024-05-20', 'Credit card fully delinquent 180+ DPD'),
(8,  1, 1, 'origination', '2020-06-01', 'Initial origination'),
(10, 1, 1, 'origination', '2020-01-01', 'Initial origination'),
(10, 1, 2, 'downgrade',   '2024-10-20', 'Payment irregularity, 35 DPD'),
(11, 1, 1, 'origination', '2018-03-01', 'Initial origination'),
(11, 1, 2, 'downgrade',   '2023-06-01', 'Counterparty financial distress'),
(11, 2, 3, 'default',     '2024-02-10', 'Borrower in bankruptcy, 210 DPD'),
(12, 1, 1, 'origination', '2019-05-01', 'Initial origination'),
(12, 1, 3, 'default',     '2024-08-01', 'Counterparty default, cross-default triggered'),
(16, 1, 1, 'origination', '2020-01-15', 'Initial origination'),
(16, 1, 2, 'downgrade',   '2024-08-10', 'Sector downturn, 60 DPD'),
(17, 1, 1, 'origination', '2020-07-01', 'Initial origination'),
(17, 1, 2, 'downgrade',   '2024-11-01', 'Rating downgrade B, late payments'),
(21, 1, 1, 'origination', '2019-09-01', 'Initial origination'),
(21, 1, 2, 'downgrade',   '2024-07-20', 'Sector risk increase, SICR trigger'),
(22, 1, 1, 'origination', '2019-02-01', 'Initial origination'),
(22, 1, 2, 'downgrade',   '2023-11-15', 'Missed payments'),
(22, 2, 3, 'default',     '2024-04-10', 'Counterparty default confirmed'),
(25, 1, 1, 'origination', '2020-06-15', 'Initial origination'),
(25, 1, 2, 'downgrade',   '2024-11-20', 'Payment 30 DPD');

-- Indexes
CREATE INDEX idx_facilities_counterparty ON facilities(counterparty_id);
CREATE INDEX idx_facilities_type ON facilities(facility_type);
CREATE INDEX idx_exposures_facility ON exposures(facility_id);
CREATE INDEX idx_exposures_reporting_date ON exposures(reporting_date);
CREATE INDEX idx_exposures_stage ON exposures(stage);
CREATE INDEX idx_ecl_exposure ON ecl_provisions(exposure_id);
CREATE INDEX idx_ecl_reporting_date ON ecl_provisions(reporting_date);
CREATE INDEX idx_ecl_stage ON ecl_provisions(stage);
CREATE INDEX idx_collateral_facility ON collateral(facility_id);
CREATE INDEX idx_staging_facility ON staging_history(facility_id);
CREATE INDEX idx_staging_effective_date ON staging_history(effective_date);

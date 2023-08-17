-- table for rfm-classification
CREATE TABLE analysis.dm_rfm_segments (
user_id INT NOT NULL,
recency INT NOT NULL,
frequency INT NOT NULL,
monetary_value INT NOT NULL,
CONSTRAINT dm_rfm_segments_pkey PRIMARY KEY (user_id),
CONSTRAINT dm_rfm_segments_recency_check CHECK (recency >= 1 AND recency <= 5),
CONSTRAINT dm_rfm_segments_frequency_check CHECK (frequency >= 1 AND frequency <= 5),
CONSTRAINT dm_rfm_segments_monetary_check CHECK (monetary_value >= 1 AND monetary_value <= 5)
);

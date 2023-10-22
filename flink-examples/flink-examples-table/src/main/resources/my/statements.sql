/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TEMPORARY TABLE `source` (
    id BIGINT,
    event_time AS CURRENT_TIMESTAMP
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '100'
);

CREATE TEMPORARY TABLE sink1 (
    id BIGINT
) WITH (
    'connector' = 'print'
);

CREATE TEMPORARY TABLE sink2 (
    event_time TIMESTAMP
) WITH (
    'connector' = 'print'
);

BEGIN STATEMENT SET;

INSERT INTO sink1 SELECT id FROM source;

INSERT INTO sink2 SELECT event_time FROM source;

END;

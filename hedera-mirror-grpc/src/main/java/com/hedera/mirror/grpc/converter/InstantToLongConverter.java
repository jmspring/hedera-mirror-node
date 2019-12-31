package com.hedera.mirror.grpc.converter;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import java.time.Instant;
import javax.inject.Named;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

@Named
@WritingConverter
public class InstantToLongConverter implements Converter<Instant, Long> {

    // Reserve 9 of the least significant digits for nanoseconds
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    @Override
    public Long convert(Instant instant) {
        if (instant == null) {
            return null;
        }
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), NANOS_PER_SECOND), instant.getNano());
    }
}
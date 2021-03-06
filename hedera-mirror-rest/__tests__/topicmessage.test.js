/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
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
'use strict';

const topicmessage = require('../topicmessage.js');
const utils = require('../utils.js');

beforeAll(async () => {
  jest.setTimeout(1000);
});

afterAll(() => {});

describe('topicmessage validateConsensusTimestampParam tests', () => {
  test('Verify validateConsensusTimestampParam returns correct result for -1234567890.000000001', () => {
    verifyInvalidConsensusTimestamp(topicmessage.validateConsensusTimestampParam(-1234567890.000000001));
  });

  test('Verify validateConsensusTimestampParam returns correct result for abc', () => {
    verifyInvalidConsensusTimestamp(topicmessage.validateConsensusTimestampParam('abc'));
  });

  test('Verify validateConsensusTimestampParam returns correct result for 1234567890', () => {
    verifyValidParamResponse(topicmessage.validateConsensusTimestampParam(1234567890));
  });

  test('Verify validateConsensusTimestampParam returns correct result for 123.0001', () => {
    verifyValidParamResponse(topicmessage.validateConsensusTimestampParam(123.0001));
  });
});

describe('topicmessage validateGetSequenceMessageParams tests', () => {
  test('Verify validateGetSequenceMessageParams returns correct result for -123', () => {
    verifyInvalidTopicAndSequenceNum(topicmessage.validateGetSequenceMessageParams(-123, -123));
  });

  test('Verify validateGetSequenceMessageParams returns correct result for abc', () => {
    verifyInvalidTopicAndSequenceNum(topicmessage.validateGetSequenceMessageParams('abc', 'abc'));
  });

  test('Verify validateGetSequenceMessageParams returns correct result for 123.0001', () => {
    verifyInvalidTopicAndSequenceNum(topicmessage.validateGetSequenceMessageParams(123.0001, 123.0001));
  });

  test('Verify validateGetSequenceMessageParams returns correct result for 0', () => {
    verifyValidParamResponse(topicmessage.validateGetSequenceMessageParams(0, 0));
  });

  test('Verify validateGetSequenceMessageParams returns correct result for 1234567890', () => {
    verifyValidParamResponse(topicmessage.validateGetSequenceMessageParams(1234567890, 1234567890));
  });
  test('Verify validateGetSequenceMessageParams returns correct result for 2', () => {
    verifyValidParamResponse(topicmessage.validateGetSequenceMessageParams(2, 1234567890));
  });
});

describe('topicmessage formatTopicMessageRow tests', () => {
  const rowInput = {
    consensus_timestamp: '1234567890000000003',
    realm_num: 1,
    topic_num: 7,
    message: {
      type: 'Buffer',
      data: [123, 34, 97, 34, 44, 34, 98, 34, 44, 34, 99, 34, 125]
    },
    running_hash: {
      type: 'Buffer',
      data: [123, 34, 99, 34, 44, 34, 100, 34, 44, 34, 101, 34, 125]
    },
    sequence_number: '3'
  };

  const formattedInput = topicmessage.formatTopicMessageRow(rowInput);

  const expectedFormat = {
    consensus_timestamp: '1234567890.000000003',
    topic_id: '0.1.7',
    message: 'eyJhIiwiYiIsImMifQ==',
    running_hash: 'eyJjIiwiZCIsImUifQ==',
    sequence_number: 3
  };

  expect(formattedInput.consensus_timestamp).toStrictEqual(expectedFormat.consensus_timestamp);
  expect(formattedInput.topic_id).toStrictEqual(expectedFormat.topic_id);
  expect(formattedInput.sequence_number).toStrictEqual(expectedFormat.sequence_number);
});

const verifyValidParamResponse = val => {
  expect(val).toStrictEqual(utils.successValidationResponse);
};

const verifyInvalidConsensusTimestamp = val => {
  expect(val).toStrictEqual(
    utils.makeValidationResponse([utils.getInvalidParameterMessageObject('consensusTimestamp')])
  );
};

const verifyInvalidTopicAndSequenceNum = val => {
  expect(val).toStrictEqual(
    utils.makeValidationResponse([
      utils.getInvalidParameterMessageObject('topic_num'),
      utils.getInvalidParameterMessageObject('sequence_number')
    ])
  );
};

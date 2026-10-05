/**
 * In-process integration tests for a broker with the client RX feature off,
 * which is the default. Nothing about the broker's behaviour may change.
 */
import test, { after, before } from 'node:test';
import assert from 'node:assert/strict';

import {
  SUBSCRIBER_PASSWORD,
  TestBroker,
  connectPublisher,
  connectSubscriber,
  createTestKeypair,
  createTestToken,
  expectLogLine,
  expectSessionClosed,
  publish,
  settle,
  startTestBroker,
  subscribe,
  waitFor,
} from './helpers/broker';

const PORT = 13702;
const SUBSCRIBER = 'ingest';

let broker: TestBroker;

before(async () => {
  // No CLIENT_RX_* variables at all: the default configuration.
  broker = await startTestBroker(PORT, {
    SUBSCRIBER_1: `${SUBSCRIBER}:${SUBSCRIBER_PASSWORD}:2`,
  });
});

after(async () => {
  await broker.stop();
});

test('the broker reports the feature as disabled', async () => {
  await expectLogLine(broker, ['[CONFIG] Client RX topics: disabled', 'allowlisted subscribers: 0']);
});

test('a client RX publish is rejected', async () => {
  const keypair = await createTestKeypair('disabled-rx-publish');
  const subscriber = await connectSubscriber(broker, SUBSCRIBER);
  const publisher = await connectPublisher(broker, keypair, await createTestToken(keypair));

  const topic = `meshcore/client/${keypair.publicKeyHex}/packets`;
  await publish(publisher, topic, { raw: 'ab'.repeat(8), lat: 1, lon: 2 });

  await expectSessionClosed(broker, publisher.clientId);
  await expectLogLine(broker, ['✗ Publish denied', topic, '(client RX topics disabled)']);
  assert.deepEqual(subscriber.messages, []);

  await subscriber.end();
});

test('subscribing to the namespace is answered exactly as before', async () => {
  const subscriber = await connectSubscriber(broker, SUBSCRIBER, []);

  assert.equal(await subscribe(subscriber, 'meshcore/client/#'), 'granted');
  await settle();
  assert.deepEqual(subscriber.messages, []);

  await subscriber.end();
});

test('observer topics are unaffected', async () => {
  const keypair = await createTestKeypair('disabled-observer');
  const subscriber = await connectSubscriber(broker, SUBSCRIBER);
  const publisher = await connectPublisher(broker, keypair, await createTestToken(keypair));

  const topic = `meshcore/SEA/${keypair.publicKeyHex}/packets`;
  const payload = { origin_id: keypair.publicKeyHex, raw: 'ef'.repeat(8) };
  await publish(publisher, topic, payload);

  await waitFor(() => subscriber.messages.length > 0, 'the observer packet to be delivered');
  assert.deepEqual(subscriber.messages, [{ topic, payload: JSON.stringify(payload) }]);

  await Promise.all([subscriber.end(), publisher.end()]);
});

# message_processor

Open-sourced part of a personal crypto market-making/arbitrage framework.

## Background

The main goal of this processor was to address the following issues:

Crypto markets move fast, and you need to be able to react to them as fast as possible.
However, some actions - such as sending an order via API - cause a noticable delay.

There is therefore a need to forward actions whenever possible, and to keep track of *what the next immediate action should be*.

For example, if a signal disappears while an order is being confirmed, a cancellation should be sent out as soon as possible.

This is what this processor aims to do, for a very high number of simultaneous strategies.

## Logic

The message processor expects various strategies to be sending order messages or cancellation messages via a Redis pub/sub channel.

These messages are then forwarded (or not) to an order broker depending on a certain logic:

- Once a message comes in, the processor checks if the strategy is ready to process a new order, i.e. it is not waiting for an order confirmation from the broker.

- If the message processor has no track of an open order, a lock is placed on the strategy and the order message is forwarded to the broker for processing. The processor then asynchronously expects an answer from the broker, and will release the lock when it arrives. It also updates the message to include the confirmed order number.

- If an open order already exists and a new order message comes in, the message processor will request a cancellation of the open order, await confirmation, and then place a new order. If a cancellation order comes in instead, no new order is placed after the cancellation.

## Gotchas

Since the market usually moves faster than orders can be processed, the last message for a strategy is kept in memory. Whenever a lock is released, a check is made for any pending messages. This ensures that the latest status is always used, regardless of how many messages came in, or how long it took to confirm an order.
The processsor also allows to place orders which do no need to be replaced dynamically. But that is less exciting so I am not expanding on it too much.

## Contact me

I love talking about this, but am not willing to open source my entire framework at this point in time. Feel free to reach out to talk more!

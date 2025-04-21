# TP-distribuidos
TP final de Sistemas Distribuidos


### aio-pika vs pika: Performance & Trade-offs
Performance Comparison
Aio-pika is typically not significantly slower than pika when used in the right context. In fact, for applications already using asyncio (like yours), it often provides better overall performance.

Individual operations: Pure synchronous pika might have slightly less overhead (5-10%) for individual operations
Overall throughput: Aio-pika typically delivers better throughput in real-world scenarios by allowing your application to handle multiple operations concurrently without blocking
Your boundary service is already using asyncio for socket operations, so aio-pika aligns better with your architecture.

Trade-offs with aio-pika
Potential Downsides
Learning curve: Async programming patterns can be more complex to understand and debug
Maturity: Pika is RabbitMQ's official Python client with longer history and more examples
Debugging: Async stacktraces can be harder to follow
Dependencies: Adds an extra dependency (not just pika but also async support)
Benefits for Your Case
Integration: Seamlessly integrates with your existing asyncio code
No blocking: Won't block your event loop during RabbitMQ operations
Simplicity: Avoids complex thread management that would be needed with synchronous pika
Scalability: Better handles concurrent connections and operations
Recommendation
Since your application is already built on asyncio (as shown in your main.py and socket handling), aio-pika is the better technical choice despite any minor overhead. Using synchronous pika would require complex thread management that would likely hurt performance and code quality more than any theoretical benefit from synchronous operations



# IMPROVMENTS

## Improvements to the Worker Class
### 1. Do not use sleep(1) in the main loop
Understanding the sleep in the Worker's Main Loop
You're right to question the sleep(1) in the main loop. This isn't an ideal pattern, but it's not necessarily wrong either.

What's happening here
This loop does two main things:

Keeps the worker's main task alive
Periodically yields control to the event loop (via await asyncio.sleep(1))
Why this works
The worker doesn't get stuck here because:

The RabbitMQ consumption runs asynchronously: The self.rabbitmq.consume() call in _setup_rabbitmq() sets up asynchronous message handling. When messages arrive, they trigger your callback function without needing the main loop to do anything.

Event-driven architecture: Your consumer callback is registered with the RabbitMQ client and will be invoked whenever a message is received, independent of this loop.

Yielding to the event loop: The await asyncio.sleep(1) releases control back to the event loop, allowing other tasks (like your message processing) to run.

Could it be better?
Yes. While this pattern works, there are some improvements you could consider:

This approach:

Avoids arbitrary polling intervals
Is more efficient (no waking up every second)
Responds immediately to shutdown signals
But yes, your current implementation with the 1-second sleep is a common pattern and works fine for most use cases. The sleep is there to avoid busy-waiting (consuming 100% CPU) while still allowing the worker to check its shutdown flag periodically.

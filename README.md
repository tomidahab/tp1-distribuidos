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
